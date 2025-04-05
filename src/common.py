import pendulum


class TimeStamp:

    def time_now(self, timezone: str = "Asia/Seoul"):

        tz = pendulum.timezone(timezone)
        return pendulum.now(tz=tz).replace(tzinfo=None)
