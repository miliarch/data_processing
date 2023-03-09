from requests.auth import AuthBase


class InvalidAuthSchemeError(Exception):
    def __init__(self, auth_scheme, valid_schemes):
        message = f"'{auth_scheme}' is invalid. "
        message += "Valid auth schemes: {valid_schemes}"
        super().__init__(message)


class TokenAuth(AuthBase):
    VALID_AUTH_SCHEMES = ['Token']

    def __init__(self, token, auth_scheme='Token'):
        self.token = token
        if auth_scheme not in self.VALID_AUTH_SCHEMES:
            raise InvalidAuthSchemeError(
                auth_scheme,
                self.VALID_AUTH_SCHEMES)
        self.auth_scheme = auth_scheme

    def __call__(self, request):
        request.headers['Authorization'] = f'{self.auth_scheme} {self.token}'
        return request
