##### ALERT : AI GENERATED CODE, MAY CONTAIN BUGS, USE WITH CAUTION #####


def idmap(user_id: int, rounds: int = 6) -> int:
    """
    Symmetric 32-bit Feistel Cipher.
    Maps [0, 2^32 - 1] perfectly onto itself.
    wikipedia.org/wiki/Feistel_cipher#Involution_Feistel_networks
    """
    # Split into two 16-bit halves
    L = (user_id >> 16) & 0xFFFF
    R = user_id & 0xFFFF

    for _ in range(rounds):
        # The Round Function F(R):
        # Can be anything. Let's use a prime multiplication and bit shift
        # to cause maximum avalanche effect. Mask with 0xFFFF to keep it 16-bit.
        F = ((R * 0x5BD1) ^ (R >> 3) ^ 0x8A2C) & 0xFFFF

        # Standard Feistel cross-wiring
        next_L = R
        next_R = L ^ F

        L = next_L
        R = next_R

    # The absolute most crucial step for an Involution:
    # A standard Feistel swaps halves on every round. To make it decrypt
    # with the EXACT same forward pass, we must omit the swap on the last
    # round (or simply swap them back here).
    return (R << 16) | L




def midmap(user_id: int, rounds: int = 4) -> int:
    """
    Symmetric 32-bit Feistel Cipher.
    Maps [0, 2^32 - 1] perfectly onto itself.
    wikipedia.org/wiki/Feistel_cipher#Involution_Feistel_networks
    """
    # Split into two 16-bit halves
    L = (user_id >> 16) & 0xFFFF
    R = user_id & 0xFFFF

    for _ in range(rounds):
        # The Round Function F(R):
        # Can be anything. Let's use a prime multiplication and bit shift
        # to cause maximum avalanche effect. Mask with 0xFFFF to keep it 16-bit.
        F = ((R * 0x5BD1) ^ (R >> 3) ^ 0x8A2C) & 0xFFFF

        # Standard Feistel cross-wiring
        next_L = R
        next_R = L ^ F

        L = next_L
        R = next_R

    # The absolute most crucial step for an Involution:
    # A standard Feistel swaps halves on every round. To make it decrypt
    # with the EXACT same forward pass, we must omit the swap on the last
    # round (or simply swap them back here).
    return (R << 16) | L




def lidmap(user_id: int, rounds: int = 5) -> int:
    """
    Symmetric 32-bit Feistel Cipher.
    Maps [0, 2^32 - 1] perfectly onto itself.
    wikipedia.org/wiki/Feistel_cipher#Involution_Feistel_networks
    """
    # Split into two 16-bit halves
    L = (user_id >> 16) & 0xFFFF
    R = user_id & 0xFFFF

    for _ in range(rounds):
        # The Round Function F(R):
        # Can be anything. Let's use a prime multiplication and bit shift
        # to cause maximum avalanche effect. Mask with 0xFFFF to keep it 16-bit.
        F = ((R * 0x5BD1) ^ (R >> 3) ^ 0x8A2C) & 0xFFFF

        # Standard Feistel cross-wiring
        next_L = R
        next_R = L ^ F

        L = next_L
        R = next_R

    # The absolute most crucial step for an Involution:
    # A standard Feistel swaps halves on every round. To make it decrypt
    # with the EXACT same forward pass, we must omit the swap on the last
    # round (or simply swap them back here).
    return (R << 16) | L