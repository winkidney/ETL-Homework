class TimeFrames:
    FIVE_MINUTES = "5m"

    _str2minutes = {
        FIVE_MINUTES: 5,
    }

    @classmethod
    def to_int(cls, minutes: str) -> int:
        return cls._str2minutes[minutes]
