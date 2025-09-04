
import string
import os
ALPHABET    = string.ascii_lowercase + string.digits
AXO_ID_SIZE = int(os.environ.get("AXO_ID_SIZE", "16"))