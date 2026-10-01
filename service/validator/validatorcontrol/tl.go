package validatorcontrol

import "github.com/xssnick/tonutils-go/tl"

const (
	schemeControlQuery = "engine.validator.controlQuery data:bytes = Object"

	schemeKeyHash           = "engine.validator.keyHash key_hash:int256 = engine.validator.KeyHash"
	schemeSignature         = "engine.validator.signature signature:bytes = engine.validator.Signature"
	schemeOneStat           = "engine.validator.oneStat key:string value:string = engine.validator.OneStat"
	schemeStats             = "engine.validator.stats stats:(vector engine.validator.oneStat) = engine.validator.Stats"
	schemeControlQueryError = "engine.validator.controlQueryError code:int message:string = engine.validator.ControlQueryError"
	schemeSuccess           = "engine.validator.success = engine.validator.Success"
	schemeJSONConfig        = "engine.validator.jsonConfig data:string = engine.validator.JsonConfig"

	schemeExportPublicKey = "engine.validator.exportPublicKey key_hash:int256 = PublicKey"
	schemeGenerateKeyPair = "engine.validator.generateKeyPair = engine.validator.KeyHash"
	schemeAddPermanentKey = "engine.validator.addValidatorPermanentKey key_hash:int256 election_date:int ttl:int = engine.validator.Success"
	schemeAddTempKey      = "engine.validator.addValidatorTempKey permanent_key_hash:int256 key_hash:int256 ttl:int = engine.validator.Success"
	schemeAddADNLAddress  = "engine.validator.addValidatorAdnlAddress permanent_key_hash:int256 key_hash:int256 ttl:int = engine.validator.Success"
	schemeSign            = "engine.validator.sign key_hash:int256 data:bytes = engine.validator.Signature"
	schemeGetStats        = "engine.validator.getStats = engine.validator.Stats"
	schemeGetConfig       = "engine.validator.getConfig = engine.validator.JsonConfig"
)

// ControlQuery is the validator-engine control envelope. Data contains one
// boxed engine.validator query.
type ControlQuery struct {
	Data []byte `tl:"bytes"`
}

type KeyHash struct {
	KeyHash []byte `tl:"int256"`
}

type Signature struct {
	Signature []byte `tl:"bytes"`
}

type OneStat struct {
	Key   string `tl:"string"`
	Value string `tl:"string"`
}

type Stats struct {
	Stats []OneStat `tl:"vector struct"`
}

type ControlQueryError struct {
	Code    int32  `tl:"int"`
	Message string `tl:"string"`
}

type Success struct{}

type JSONConfig struct {
	Data string `tl:"string"`
}

type ExportPublicKey struct {
	KeyHash []byte `tl:"int256"`
}

type GenerateKeyPair struct{}

type AddValidatorPermanentKey struct {
	KeyHash      []byte `tl:"int256"`
	ElectionDate uint32 `tl:"int"`
	TTL          uint32 `tl:"int"`
}

type AddValidatorTempKey struct {
	PermanentKeyHash []byte `tl:"int256"`
	KeyHash          []byte `tl:"int256"`
	TTL              uint32 `tl:"int"`
}

type AddValidatorADNLAddress struct {
	PermanentKeyHash []byte `tl:"int256"`
	KeyHash          []byte `tl:"int256"`
	TTL              uint32 `tl:"int"`
}

type Sign struct {
	KeyHash []byte `tl:"int256"`
	Data    []byte `tl:"bytes"`
}

type GetStats struct{}

type GetConfig struct{}

func init() {
	tl.Register(ControlQuery{}, schemeControlQuery)

	tl.Register(KeyHash{}, schemeKeyHash)
	tl.Register(Signature{}, schemeSignature)
	tl.Register(OneStat{}, schemeOneStat)
	tl.Register(Stats{}, schemeStats)
	tl.Register(ControlQueryError{}, schemeControlQueryError)
	tl.Register(Success{}, schemeSuccess)
	tl.Register(JSONConfig{}, schemeJSONConfig)

	tl.Register(ExportPublicKey{}, schemeExportPublicKey)
	tl.Register(GenerateKeyPair{}, schemeGenerateKeyPair)
	tl.Register(AddValidatorPermanentKey{}, schemeAddPermanentKey)
	tl.Register(AddValidatorTempKey{}, schemeAddTempKey)
	tl.Register(AddValidatorADNLAddress{}, schemeAddADNLAddress)
	tl.Register(Sign{}, schemeSign)
	tl.Register(GetStats{}, schemeGetStats)
	tl.Register(GetConfig{}, schemeGetConfig)
}
