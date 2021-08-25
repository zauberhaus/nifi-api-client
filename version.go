package nifi

var ClientVersion *Version

type Version struct {
	Version   string `json:"version"`
	GitCommit string `json:"commit"`
	BuildTime string `json:"buildTime"`
	TreeState string `json:"treeState"`

	OS        string `json:"os"`
	Arch      string `json:"arch"`
	GoVersion string `json:"goVersion"`
}
