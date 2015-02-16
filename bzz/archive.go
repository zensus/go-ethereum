package bzz

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	// "path"
	"path/filepath"
)

// -default: default file fallback with 404 : index.html
// manifest-path: manifest itself is public and is under: /manifest.json
// manifest-template: manifest template: the directory is merged into this template (it will typically contain external links or assets)
// redirect (download is default = ethercrawl) external links are interpreted by the browser as redirects. By default the uploader downloads, stores and embeds the content. The resulting root key will be shown as hostname.
// register-names use eth://NameReg/... to register paths with names

type StoreF func(path, contentType string) (key Key, err error)

func DpaF(dpa *DPA) (f StoreF) {
	f = func(path, contentType string) (key Key, err error) {
		var file *os.File
		file, err = os.Open(path)
		if err != nil {
			fmt.Printf("Error opening file '%s': %v\n", path, err)
		}

		stat, err := file.Stat()
		if err != nil {
			fmt.Printf("Error file stat for '%s': %v\n", path, err)
		}
		sr := io.NewSectionReader(file, 0, stat.Size())
		return dpa.Store(sr, nil)
	}
	return
}

func ProxyF(url string) (f StoreF) {
	f = func(path, contentType string) (key Key, err error) {
		var file *os.File
		file, err = os.Open(path)
		if err != nil {
			fmt.Printf("Error opening file '%s': %v\n", path, err)
		}
		var resp *http.Response
		resp, err = http.Post(url, contentType, file)
		key = make([]byte, 32)
		resp.Body.Read(key)
		return
	}
	return
}

func (self *ArcHive) makeVisit(notFound string, f StoreF) (visitF func(path string, f os.FileInfo, err error) error) {
	visitF = func(path string, info os.FileInfo, ferr error) (err error) {
		if ferr != nil {
			return ferr
		}
		var hash []byte
		contentType := ""
		// contentType := mimetype(path)
		hash, err = f(path, contentType)
		if err != nil {
			return
		}
		entry := manifestEntry{
			Path:        path,
			Hash:        string(hash),
			ContentType: contentType,
		}
		self.Template.Entries = append(self.Template.Entries, entry)
		dir := ""
		basename := path
		// dir, basename := parse(path);
		if basename == notFound {
			entry := manifestEntry{
				Path:        dir,
				Hash:        string(hash),
				ContentType: contentType,
				Status:      404,
			}
			self.Template.Entries = append(self.Template.Entries, entry)
		}
		return
	}
	return
}

func TemplateFromFile(templateFile string) ([]byte, error) {

	return ioutil.ReadFile(templateFile)
}

type ArcHive struct {
	Template *manifest // template/config/manifest/server/
	// register string
	Key Key
}

func NewArcHive(templateData []byte) (self *ArcHive, err error) {
	var template *manifest
	err = json.Unmarshal(templateData, template)
	if err != nil {
		dpaLogger.Warnf("Error parsing json template '%v': %v\n", template, err)
		return
	}

	self = &ArcHive{
		Template: template,
		// register: host,
	}
	return
}

/*
 Run(path, notFound string, register bool, f StoreF) (key Key, man *manifest, err error)
 path specifies the directory where relative paths are derived from or matched against
 notFound is the name of the file in any directory that is used as errorpage if an asset is  not found. Routes that has no manifest entry are routed to their longest prefix.
 StoreF is the function that is called on each file or directory , typically either native (using a DPA swarm store directly, e.g., in Mist component) or proxy (using swarm http proxy, in command line tool)
*/
func (self *ArcHive) Run(path, notFound string, f StoreF) (err error) {

	visitF := self.makeVisit(notFound, f)
	err = filepath.Walk(path, visitF)

	if err != nil {
		dpaLogger.Warnf("Error scanning files in '%v': %v\n", path, err)
	}
	return
}

func (self *ArcHive) Save(filename string, f StoreF) (key []byte, err error) {
	// var file *os.File
	// file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	var manData []byte
	manData, err = self.toJSON()
	if err != nil {
		return
	}
	ioutil.WriteFile(filename, manData, os.ModePerm)
	key, err = f(filename, "application/bzz-sitemap+json")
	if err != nil {
		dpaLogger.Warnf("Error storing sitemap manifest to '%s': %v", filename, err)
	}
	return
}

func (self *ArcHive) toJSON() (manData []byte, err error) {
	manData, err = json.Marshal(self.Template)
	if err != nil {
		dpaLogger.Warnf("Error generating json manifest: %v\n", err)
	}
	return
}
