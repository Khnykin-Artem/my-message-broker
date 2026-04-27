package auth

type Authenticator struct {
    tokens map[string]bool
}

func NewAuthenticator(tokens []string) *Authenticator {
    m := make(map[string]bool)
    for _, t := range tokens {
        m[t] = true
    }
    return &Authenticator{tokens: m}
}

func (a *Authenticator) Authenticate(token string) bool {
    return a.tokens[token]
}