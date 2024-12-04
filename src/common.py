import pendulum


class TimeStamp:

    tz = pendulum.timezone("Asia/Seoul")

    def time_now(self):
        return pendulum.now(tz=self.tz)
