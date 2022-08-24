package tcli

///////////////////// scan options //////////////////////
var (
	ScanOptKeyOnly      string = "key-only"
	ScanOptCountOnly    string = "count-only"
	ScanOptLimit        string = "limit"
	ScanOptStrictPrefix string = "strict-prefix"
)

// for completer to work, keyword list
var ScanOptsKeywordList = []string{
	ScanOptKeyOnly,
	ScanOptCountOnly,
	ScanOptLimit,
	ScanOptStrictPrefix,
}

///////////////////// end of scan options ///////////////

//////////////// del/delp/delall options ////////////////
var (
	DeleteOptWithPrefix string = "prefix-mode"
	DeleteOptBatchSize  string = "batch-size"
	DeleteOptLimit      string = "limit"
	DeleteOptYes        string = "yes"
)

var DeleteOptsKeywordList = []string{
	DeleteOptWithPrefix,
	DeleteOptBatchSize,
	DeleteOptLimit,
	DeleteOptYes,
}

//////////////// end of del/delp/delall options ////////

///////////////// loadcsv options //////////////////////
var (
	LoadFileOptBatchSize string = "batch-size"
)

var LoadFileOptsKeywordList = []string{
	LoadFileOptBatchSize,
}

//////////////// end of loadcsv options ///////////////

///////////////// backup options /////////////////////
var (
	BackupOptBatchSize string = "batch-size"
)

var BackupOptsKeywordList = []string{
	BackupOptBatchSize,
}

//////////////// end of backup options ///////////////
