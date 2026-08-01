package p2p

import "github.com/xssnick/tonutils-go/tl"

func parseQUICOverlayObject(
	payload []byte,
	envelope tl.Serializable,
) (tl.Serializable, error) {
	body, err := parseQUICOverlayBody(payload, envelope)
	if err != nil {
		return nil, err
	}
	return parseOneQUICOverlayObject(body)
}
