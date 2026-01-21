#!/usr/bin/env python3

import json
import time
from jwcrypto import jwt, jwk

def generate_jwt_from_jwk(jwk_json_str, payload_claims):
    """
    Generates a signed JWT using a private key in JWK format.
    """
    try:
        # 1. Load the JWK from the JSON string
        key_dict = json.loads(jwk_json_str)
        key = jwk.JWK(**key_dict)

        # 2. Define the Header
        # Ideally, match the 'alg' to the key type (e.g., RSA -> RS256, EC -> ES256)
        header = {
            "alg": "RS256",
            "typ": "JWT",
            "kid": key.key_id  # Include Key ID if present in the JWK
        }

        # 3. Create the JWT Object
        token = jwt.JWT(header=header, claims=payload_claims)

        # 4. Sign the token using the private key object
        token.make_signed_token(key)

        # 5. Serialize to get the final string
        return token.serialize()

    except Exception as e:
        print(f"Error generating token: {e}")
        return None

# --- USAGE EXAMPLE ---

if __name__ == "__main__":
    # In a real scenario, you would load your private JWK string from a file or env var.
    # For this demo, we generate a fresh RSA key on the fly so the script runs immediately.
    private_jwk_json = """
    {
        "p": "6sv8vwspDekbubwrco_yee98nDFCf9r7K4brkEKdtOOqMFQrCcwbugk5yPpi4qrttF4rn1ByB1cppYFTTRmNpSITZvNkc-EXlookEQ1slJTorSHARQNc8TVMvxU6nUe9TehDqaQw1TL9kblgnNxDc-HbS3e5ZLYIKkO7aCq4nGM",
        "kty": "RSA",
        "q": "kkvbvJjgbMHIxX91av32_DpR2b8jO1TWuHGwV-yQi22H05dtEqIYOcY9z-sN_w_-1zM0ygjkMnnGU7-SW1lAWXS6haBsOM4gVP7FQ-OqL1j_cnkAN48RLibIt758A7yKcMRNJA6u7ouU-g-Zs6yIjxesXfqGrSzSU4oeiNISPf8",
        "d": "RkhWNvMa47B2h-TK11YqGi1r_A8yOX9hk86V8d-cgrnckdYFnhSLlcLjoKYkcPBlzmMcWrCs5ykvalit16jN-nFqOQXOI0UFgoaJinuUtnUvfWHJa4BBo1aq0sPbtvRMC2qnbgcJdCJR5uO7dKdjoFy4RmqZQ3osAhirdrgl4gPGqWqPHrgJyYJPL1MhpHb-g2ZhtotnhHPYAK3VD6EqZZwOesEuyA9mJyZY3fOAz1s99Wwv8Fw9o4Gu2cpkzrmTaD43zZVOj_SZHf0jIXBvFCNj8Z0SaICJJcgOmu3mIxcWPbGgiKAKjuCSKGmmxLj9nobhvpYFJtDn8rIKysy1oQ",
        "e": "AQAB",
        "use": "sig",
        "kid": "sig-2025-12-12T15:52:22Z",
        "qi": "ZeboCP78M6VyxLcxXfaeAKlSuGXHAKtg6zOBC0Dn4VtFe1vCuwteqZAFPYCZJh8xBYlMrCq40ut3eOb-xf_qJ3xetUWPJ0U9h9TG2HzgTv5NZUuqg79j7_rw5PzaYerIG2QW9gwkCdotW_5OgWUnH-ELhmx80_Q8OEksvc1Myqo",
        "dp": "w9pyMr3RegwHl4_Rwhc20OWm0Pb6HIKCXxWFK2mV-YyqqvOajuqV-kG11OKfV6ny7DBdPOAyrdLUJ31QChEVqThabNb75PlO3sDOQvcqqmnoCHsN0cNzZLTsFrxTj1yHGRR0VG5kWYLWJxc18sJ89Y3hifsNR2fcOb0T91kjczc",
        "alg": "RS256",
        "dq": "kU6zdIHL93oKts_AioKx_RjYD5Ufo2DC3PRfGRWpBDPIg0uWVLmXolrbLlbj0gHLN2hu-HUYY2I8sRZIgl8F4VRlpzAODeX-iy16NdI9SUX2g3bX1ldN0y9GkeqrNvLf9t2jWTsUWW9ei3lPSv0FrkrvM3EQr5UjW0KGzZMJ93U",
        "n": "hi3pcXuRrhy2gHjkHCq6JBcxpeC5udojlLukmsyAZ7x1vHwHTx_9c6s73XhfJoRBnd3V3SL1JAUuBiVJ68Pnrtuztp5j8Cdw33S5jKf-8PranMucG_4wWejWm1gbKomZBm79Baf_fJkDOwumsTXvO9dgX7c2dHtA0oO15eVdzSDf7azgLU7OvVukiE71tqoQwnPgK47xDvjw83ZN8eWZzDPGpA9DVoEjhwOeasDgUdVv1L-b2uOqCMYcLixYJ58O48cQSEOreIi1FbIJGoT19VKtJ528JrTkKihZb-BmFIvyIu36lGRpeJ1pbarIMx-sC_-UfKEawHmYQp7MwB1dnQ"
    }
    """
    # test_key = jwk.JWK.generate(kty='RSA', size=2048, kid='my-test-key-id')
    # private_jwk_json = test_key.export_private()

    # Define the Token Payload (Claims)
    my_claims = {
        "sub": "user_12345",
        "name": "John Doe",
        "role": "admin",
        "iat": int(time.time()),             # Issued At
        "exp": int(time.time()) + 86400,     # Expires in 1 day
        "aud": ["mcp-gateway"],
        "iss": "https://auth.example.com"
    }

    # Generate the Token
    jwt_token = generate_jwt_from_jwk(private_jwk_json, my_claims)

    print("\n--- Generated JWT ---")
    print(jwt_token)
