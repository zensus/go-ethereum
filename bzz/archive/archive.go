// arcHive
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/ethereum/go-ethereum/bzz"
)

// -default: default file fallback with 404 : index.html
// manifest-path: manifest itself is public and is under: /manifest.json
// manifest-template: manifest template: the directory is merged into this template (it will typically contain external links or assets)
// redirect (download is default = ethercrawl) external links are interpreted by the browser as redirects. By default the uploader downloads, stores and embeds the content. The resulting root key will be shown as hostname.
// register-names use eth://NameReg/... to register paths with names

var (
	proxy         string
	notFound      string
	template      string
	redirect      bool
	registerNames bool
	withoutScan   bool
	withoutUpload bool
	output        string
	dir           string
)

func Init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s [options] [path]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&proxy, "proxy", "", "swarm http endpoint to send requests to")
	flag.StringVar(&notFound, "not-found", "", "fallback filename used as error pagge if url path does not match an entry")
	flag.StringVar(&template, "template", "", "template for server manifest ('' = no template)")
	flag.StringVar(&output, "output", "", "output path for final manifest")
	flag.BoolVar(&withoutScan, "without-scan", false, "scan directory and add all readable content to manifest (false, only consider paths given in template) ")
	flag.BoolVar(&withoutUpload, "without-upload", false, "if true, files are not uploaded, only hashes are calculated a manifest is created (false, upload every asset to swarm)")
	flag.BoolVar(&redirect, "redirect", false, "redirect external links as opposed to download")
	flag.BoolVar(&registerNames, "register-names", false, "use nameReg to register host names with their hash")
	flag.Parse()
	dir = flag.Arg(0)
}

// (path string, notFound string, register bool, f storeF) (b []byte, err error)
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	Init()
	templateData, err := bzz.TemplateFromFile(template)
	if err != nil {
		fmt.Printf("Error opening manifest template file '%s': %v\n", template, err)
		return
	}
	var arcHive *bzz.ArcHive
	arcHive, err = bzz.NewArcHive(templateData)
	var storeF bzz.StoreF
	if !withoutUpload {
		bzz.ProxyF(proxy)
	}
	var status int
	err = arcHive.Run(dir, notFound, storeF)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		status = 1
	} else {
		var key []byte
		key, err = arcHive.Save(output, storeF)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		} else {
			fmt.Printf("Server sitemap manifest written to '%s'\n", output)
			fmt.Printf("Server sitemap manifest root key: %x\n", key)
		}
	}
	os.Exit(status)
}
