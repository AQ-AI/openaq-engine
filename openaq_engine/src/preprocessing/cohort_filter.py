import pandas as pd


class Filter:
    @staticmethod
    def filter_non_null_pm25_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out rows which are non null

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with 0 values
        """

        return (
            df.assign(
                null_values=(
                    df.value.apply(lambda pm25_value: float(pm25_value) >= 0)
                )
            )
            .query("null_values == False")
            .drop(["null_values"], axis=1)
        )

    @staticmethod
    def filter_extreme_pollution_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out rows which contain extremely high pm25 values

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with extreme values removed
        """

        return (
            df.assign(
                extreme_values=(
                    df.value.apply(lambda pm25_value: float(pm25_value) <= 500)
                )
            )
            .query("extreme_values == False")
            .drop(["extreme_values"], axis=1)
        )
