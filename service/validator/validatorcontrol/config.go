package validatorcontrol

import (
	"encoding/base64"
	"encoding/json"
	"sort"
)

type jsonValidatorConfig struct {
	Type       string          `json:"@type"`
	ADNL       []jsonADNL      `json:"adnl"`
	Validators []jsonValidator `json:"validators"`
}

type jsonADNL struct {
	Type     string `json:"@type"`
	ID       string `json:"id"`
	Category int32  `json:"category"`
}

type jsonValidator struct {
	Type          string                     `json:"@type"`
	ID            string                     `json:"id"`
	TempKeys      []jsonValidatorTempKey     `json:"temp_keys"`
	ADNLAddresses []jsonValidatorADNLAddress `json:"adnl_addrs"`
	ElectionDate  uint32                     `json:"election_date"`
	ExpireAt      uint32                     `json:"expire_at"`
}

type jsonValidatorTempKey struct {
	Type     string `json:"@type"`
	Key      string `json:"key"`
	ExpireAt uint32 `json:"expire_at"`
}

type jsonValidatorADNLAddress struct {
	Type     string `json:"@type"`
	ID       string `json:"id"`
	ExpireAt uint32 `json:"expire_at"`
}

func (s *Server) configJSON() (string, error) {
	entries := s.keys.Entries()
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ElectionDate != entries[j].ElectionDate {
			return entries[i].ElectionDate < entries[j].ElectionDate
		}

		return compareKeyIDs(entries[i].ID, entries[j].ID) < 0
	})

	validators := make([]jsonValidator, 0, len(entries))
	for _, entry := range entries {
		if !entry.Permanent {
			continue
		}

		keyID := base64.StdEncoding.EncodeToString(entry.ID[:])
		tempKeys := make([]jsonValidatorTempKey, 0, 1)
		if entry.TempExpireAt != 0 {
			tempKeys = append(tempKeys, jsonValidatorTempKey{
				Type:     "engine.validatorTempKey",
				Key:      keyID,
				ExpireAt: entry.TempExpireAt,
			})
		}

		adnlAddresses := make([]jsonValidatorADNLAddress, 0, 1)
		if entry.HasADNL {
			adnlAddresses = append(adnlAddresses, jsonValidatorADNLAddress{
				Type:     "engine.validatorAdnlAddress",
				ID:       base64.StdEncoding.EncodeToString(entry.ADNLID[:]),
				ExpireAt: entry.ADNLExpireAt,
			})
		}

		validators = append(validators, jsonValidator{
			Type:          "engine.validator",
			ID:            keyID,
			TempKeys:      tempKeys,
			ADNLAddresses: adnlAddresses,
			ElectionDate:  entry.ElectionDate,
			ExpireAt:      entry.PermanentExpireAt,
		})
	}

	data, err := json.Marshal(jsonValidatorConfig{
		Type: "engine.validator.config",
		ADNL: []jsonADNL{{
			Type:     "engine.adnl",
			ID:       base64.StdEncoding.EncodeToString(s.localADNLID[:]),
			Category: 0,
		}},
		Validators: validators,
	})
	if err != nil {
		return "", err
	}

	return string(data), nil
}
