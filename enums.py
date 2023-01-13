import enum


class HttpMethod(enum.Enum):
    GET = "get"
    POST = "post"


class Currency(enum.IntEnum):
    ETH = 0
    SOL = 1
    AVAX = 2

    def to_chain_name(self) -> str:
        return CURRENCY_CHAIN_NAME_MAPPING[self]


CURRENCY_CHAIN_NAME_MAPPING = {
    Currency.ETH: "ethereum",
    Currency.SOL: "solana",
    Currency.AVAX: "avalanche",
}
