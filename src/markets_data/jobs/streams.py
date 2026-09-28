"""Both Kappa streams in one Spark application (run by spark-submit, as this tenant, for as long
as it runs): the trades stream and the card-authorisation stream, four queries in all.

One JVM instead of two saves ~1 GB on the laptop the platform runs on (a Spark driver carries
~450 MB beside its heap). Each query keeps its own checkpoint, so either stream can still be
replayed alone; any query failing stops the job, and the platform restarts the service.
"""

from markets_data.jobs import card_auths_stream, trades_stream
from markets_data.spark import session
from markets_data.streaming import watch


def main() -> None:
    spark = session("markets-streams")
    watch(*trades_stream.start(spark), *card_auths_stream.start(spark))


if __name__ == "__main__":
    main()
