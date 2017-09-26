package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// defaultBufSize controls the size of reads and writes - Azure performs best
// when things are done in large chunks.
const defaultBufSize = 32 * 1024 * 1024

// Stemcell

func StemcellFilename(version, os string) string {
	return fmt.Sprintf("bosh-stemcell-%s-azure-hyperv-windows%s-trusty-go_agent.tgz",
		version, os)
}

func CreateManifest(filename, version, winOS, hash string) error {
	const format = `---
name: bosh-azure-hyperv-windows%[2]s-go_agent
version: %[1]s
bosh_protocol: 1
sha1: %[3]s
operating_system: windows%[2]s
cloud_properties:
  name: bosh-azure-hyperv-windows%[2]s-go_agent
  version: %[1]s
  infrastructure: azure
  hypervisor: hyperv
  disk: 40000
  disk_format: vhd
  container_format: bare
  os_type: windows
  os_distro: windows
  architecture: x86_64
  root_device_name: "/dev/sda1"
`

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, format, version, winOS, hash); err != nil {
		return err
	}
	return nil
}

func AddFileToArchive(tw *tar.Writer, name, newName string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return err
	}
	if newName != "" {
		hdr.Name = newName
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	buf := make([]byte, defaultBufSize)
	if _, err := io.CopyBuffer(tw, f, buf); err != nil {
		return err
	}
	return nil
}

func Sha1sum(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha1.New()
	buf := make([]byte, defaultBufSize)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func CreateStemcell(dirname, image, winOS, version string) error {
	if err := os.MkdirAll(dirname, 0644); err != nil {
		return err
	}
	hash, err := Sha1sum(image)
	if err != nil {
		return err
	}

	manifest := filepath.Join(dirname, "stemcell.MF")
	if err := CreateManifest(manifest, version, winOS, hash); err != nil {
		return err
	}

	stemcellName := StemcellFilename(version, winOS)
	name := filepath.Join(dirname, stemcellName)

	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	errorf := func(format string, a ...interface{}) error {
		f.Close()
		os.Remove(name)
		return fmt.Errorf(format, a...)
	}

	bufw := bufio.NewWriterSize(f, defaultBufSize)
	gw := gzip.NewWriter(bufw)
	tw := tar.NewWriter(gw)

	if err := AddFileToArchive(tw, manifest, ""); err != nil {
		return errorf("creating stemcell: %s", err)
	}
	if err := AddFileToArchive(tw, image, "image"); err != nil {
		return errorf("creating stemcell: %s", err)
	}
	if err := tw.Close(); err != nil {
		return errorf("creating stemcell: %s", err)
	}
	if err := gw.Close(); err != nil {
		return errorf("creating stemcell: %s", err)
	}
	if err := bufw.Flush(); err != nil {
		return errorf("creating stemcell: %s", err)
	}

	return nil
}

// Image

func CreateImage(vhdpath, imagepath string) error {

	if _, err := exec.LookPath("pigz"); err != nil {
		return err
	}

	// open/create files

	image, err := os.OpenFile(imagepath, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer image.Close()

	vhd, err := os.OpenFile(vhdpath, os.O_RDONLY, 0)
	if err != nil {
		image.Close()
		os.Remove(imagepath)
		return err
	}
	defer vhd.Close()

	// create pigz command

	// pipe for connecting our tar writer to pigz
	pr, pw := io.Pipe()

	// pigz has lots of very small writes so buffer writes to
	// the image file to reduce IOPS and increase throughput
	wbuf := bufio.NewWriterSize(image, 1024*64)

	var stderr bytes.Buffer

	cmd := exec.Command("pigz", "-c")
	cmd.Stdin = pr
	cmd.Stderr = &stderr
	cmd.Stdout = wbuf

	// helper func for cleanup
	exit := func(err error) error {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		image.Close()
		os.Remove(imagepath)
		return fmt.Errorf("creating image: %s", err)
	}

	// start command

	tw := tar.NewWriter(pw) // tar writer

	errCh := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		exit(err)
	}
	go func() { errCh <- cmd.Wait() }()

	// create image tarball

	fi, err := vhd.Stat()
	if err != nil {
		return exit(err)
	}
	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return exit(err)
	}
	// force name to be root.vhd
	hdr.Name = "root.vhd"
	if err := tw.WriteHeader(hdr); err != nil {
		return exit(err)
	}
	// use our own buffer so we can control the size of reads
	// from the vhd file.
	copyBuf := make([]byte, defaultBufSize)
	if _, err := io.CopyBuffer(tw, vhd, copyBuf); err != nil {
		return exit(err)
	}
	if err := tw.Close(); err != nil {
		return exit(err)
	}

	// close pipe writer - causing the reader attached to
	// the stdin of pigz to return EOF and stop pigz
	if err := pw.Close(); err != nil {
		return exit(err)
	}

	// wait for command exit

	select {
	case e := <-errCh:
		if e != nil {
			return exit(fmt.Errorf("%s: %s  --- BEGIN STDERR ---\n%s\n--- END STDERR ---",
				filepath.Base(cmd.Path), e, stderr))
		}
	case <-time.After(time.Second * 30):
		return exit(fmt.Errorf("%s: timed out waiting for exit",
			filepath.Base(cmd.Path)))
	}

	// flush any pending dating in the buffer wrapping the image file
	if err := wbuf.Flush(); err != nil {
		return exit(err)
	}
	return nil
}

// Download

func AzCopyURL(rawURL string) (source, pattern string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}
	u.RawQuery = ""
	u.Path, pattern = path.Split(u.Path)
	source = u.String()
	return
}

func DownloadVHD(vhdURL, dirname string) (string, error) {
	if _, err := os.Stat(WorkDir); os.IsNotExist(err) {
		return "", fmt.Errorf("download: invalid dirname (%s): %s", dirname, err)
	}
	source, pattern, err := AzCopyURL(VhdURL)
	if err != nil {
		return "", err
	}
	cmd := exec.Command("AzCopy.exe",
		"/Source:"+source,
		"/Pattern:"+pattern,
		"/SourceKey:"+SourceKey,
		"/Dest:"+dirname,
	)
	var stderr bytes.Buffer
	cmd.Stdout = os.Stdout // WARN
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s: %s  --- BEGIN STDERR ---\n%s\n--- END STDERR ---",
			filepath.Base(cmd.Path), err, stderr)
	}

	return filepath.Join(dirname, pattern), nil
}

// Main and flag validation

var (
	// Add env keys first so that flags take precedence
	VhdURL      = os.Getenv("AZURE_VHD_URL")      // vhd
	VhdURLFile  = os.Getenv("AZURE_VHD_URL_FILE") // vhdfile
	SourceKey   = os.Getenv("AZURE_SOURCE_KEY")   // key
	Version     = os.Getenv("AZURE_VERSION")      // version
	VersionFile = os.Getenv("AZURE_VERSION_FILE") // versionfile
	WindowsOS   = os.Getenv("AZURE_WINDOWS_OS")   // os
	Destination = os.Getenv("AZURE_DESTINATION")  // dest
	WorkDir     = os.Getenv("AZURE_TEMP_DIR")     // temp
)

func parseFlags() error {
	flag.StringVar(&VhdURL, "vhd", "", "URL to VHD, env: AZURE_VHD_URL")
	flag.StringVar(&VhdURLFile, "vhdfile", "", "File containing the VHD URL, env: AZURE_VHD_URL_FILE")
	flag.StringVar(&SourceKey, "key", "", "Azure source key for downloading blob, env: AZURE_SOURCE_KEY")
	flag.StringVar(&Version, "version", "", "Stemcell version, env: AZURE_VERSION")
	flag.StringVar(&VersionFile, "versionfile", "", "File containing the stemcell version, env: AZURE_VERSION_FILE")
	flag.StringVar(&WindowsOS, "os", "", "Windows version (2012R2 or 2016), env: AZURE_WINDOWS_OS")
	flag.StringVar(&Destination, "dest", "", "Destination directory if not provided the current working directory will be used, env: AZURE_DESTINATION")
	flag.StringVar(&WorkDir, "temp", "", "Temporary directory (must be capable of storing +130GB of data), env: AZURE_TEMP_DIR")

	flag.Parse()

	if Destination == "" {
		wd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("getting working directory: %s", err)
		}
		Destination = wd
	}

	if Version == "" && VersionFile != "" {
		src, err := ioutil.ReadFile(VersionFile)
		if err != nil {
			return fmt.Errorf("reading version file (%s): %s", VersionFile, err)
		}
		Version = string(bytes.TrimSpace(src))
	}

	if VhdURL == "" && VhdURLFile != "" {
		src, err := ioutil.ReadFile(VhdURLFile)
		if err != nil {
			return fmt.Errorf("reading VHD file (%s): %s", VhdURLFile, err)
		}
		VhdURL = string(bytes.TrimSpace(src))
	}

	return nil
}

func validateVersion(s string) error {
	if s == "" {
		return errors.New("missing required argument 'version'")
	}
	patterns := []string{
		`^\d{1,}\.\d{1,}$`,
		`^\d{1,}\.\d{1,}-build\.\d{1,}$`,
		`^\d{1,}\.\d{1,}\.\d{1,}$`,
		`^\d{1,}\.\d{1,}\.\d{1,}-build\.\d{1,}$`,
	}
	for _, pattern := range patterns {
		if regexp.MustCompile(pattern).MatchString(s) {
			return nil
		}
	}
	return fmt.Errorf("invalid version (%s) expected format [NUMBER].[NUMBER] or "+
		"[NUMBER].[NUMBER].[NUMBER]", s)
}

func validateFlags() []error {
	var errs []error
	add := func(err error) {
		errs = append(errs, err)
	}

	if err := validateVersion(Version); err != nil {
		add(err)
	}
	if VhdURL == "" {
		add(errors.New("missing required argument: [vhd]"))
	}
	if SourceKey == "" {
		add(errors.New("missing required argument: [key]"))
	}
	if WindowsOS == "" {
		add(errors.New("missing required argument: [os]"))
	}
	switch strings.ToLower(WindowsOS) {
	case "2012r2", "2016":
	// Ok
	case "windows2012r2", "windows2016":
		WindowsOS = strings.TrimPrefix(WindowsOS, "windows")
	default:
		add(fmt.Errorf("OS version must be either 2012R2 or 2016 have: %s", WindowsOS))
	}
	if Destination == "" {
		add(errors.New("missing required argument: [dest]"))
	}
	if WorkDir == "" {
		add(errors.New("missing required argument: [temp]"))
	}
	name := filepath.Join(Destination, StemcellFilename(Version, WindowsOS))
	if _, err := os.Stat(name); err == nil {
		add(fmt.Errorf("output file (%s): already exists - refusing to overwrite", name, err))
	}
	return errs
}

func realMain() error {
	if err := parseFlags(); err != nil {
		return fmt.Errorf("parsing flags: %s", err)
	}

	if errs := validateFlags(); errs != nil {
		// combine into one error message
		var buf bytes.Buffer
		fmt.Fprintln(&buf, "Error: invalid arguments")
		for _, e := range errs {
			fmt.Fprintf(&buf, "  %s\n", e)
		}
		return errors.New(buf.String())
	}

	if err := os.MkdirAll(WorkDir, 0744); err != nil {
		return fmt.Errorf("working directory: %s", err)
	}
	if err := os.MkdirAll(Destination, 0744); err != nil {
		return fmt.Errorf("destination directory : %s", err)
	}

	tempdir, err := ioutil.TempDir(WorkDir, "stemcell-")
	if err != nil {
		return fmt.Errorf("creating temp dir: %s", err)
	}
	defer os.RemoveAll(tempdir)

	vhdpath, err := DownloadVHD(VhdURL, tempdir)
	if err != nil {
		return fmt.Errorf("downloading VHD: %s", err)
	}

	imagepath := filepath.Join(tempdir, "image")
	if err := CreateImage(vhdpath, imagepath); err != nil {
		return err
	}

	if err := CreateStemcell(Destination, imagepath, WindowsOS, Version); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := realMain(); err != nil {
		FatalSkip(2, err)
	}
}

func Fatal(err interface{}) {
	FatalSkip(2, err)
}

func FatalSkip(skip int, err interface{}) {
	if err == nil {
		return
	}
	msg := "Error"
	if _, file, line, ok := runtime.Caller(skip); ok && file != "" {
		msg = fmt.Sprintf("Error (%s:%d)", filepath.Base(file), line)
	}
	switch err.(type) {
	case error, string, fmt.Stringer:
		fmt.Fprintf(os.Stderr, "%s: %s", msg, err)
	default:
		fmt.Fprintf(os.Stderr, "%s: %#v\n", msg, err)
	}
	os.Exit(1)
}
