from typing import List

import pandas as pd


class Filter:
    @staticmethod
    def filter_pollutant(
        df: pd.DataFrame, pollutant_to_predict: str
    ) -> pd.DataFrame:
        """
        Filters the DataFrame for rows containing the specified pollutant.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing air quality data.
        pollutant_to_predict : str
            The pollutant to filter for.

        Returns
        -------
        pd.DataFrame
            The filtered DataFrame containing only rows with the specified pollutant.
        """
        return (
            df.assign(
                selected_pollutant=(
                    df.parameter.apply(
                        lambda pollutant: pollutant_to_predict
                        in str(pollutant)
                    )
                )
            )
            .query("selected_pollutant == True")
            .drop(["selected_pollutant"], axis=1)
        )

    @staticmethod
    def filter_no_coordinates(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the DataFrame to remove rows with empty coordinates.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing coordinate data.

        Returns
        -------
        pd.DataFrame
            The filtered DataFrame with rows that have empty coordinates removed.
        """
        return (
            df.assign(
                no_coords=(
                    df.coordinates.apply(lambda coords: str(coords) == "{}")
                )
            )
            .query("no_coords == False")
            .drop(["no_coords"], axis=1)
        )

    @staticmethod
    def filter_non_null_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the DataFrame to remove rows with non-positive values.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing air quality data.

        Returns
        -------
        pd.DataFrame
            The filtered DataFrame with rows containing non-positive values removed.
        """
        return (
            df.assign(
                non_null_values=(
                    df.value.apply(lambda pm25_value: float(pm25_value) >= 0)
                )
            )
            .query("non_null_values == True")
            .drop(["non_null_values"], axis=1)
        )

    @staticmethod
    def filter_extreme_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the DataFrame to remove rows with extreme PM2.5 values.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing air quality data.

        Returns
        -------
        pd.DataFrame
            The filtered DataFrame with rows containing extreme PM2.5 values removed.
        """
        return (
            df.assign(
                non_extreme_values=(
                    df.value.apply(lambda pm25_value: float(pm25_value) <= 500)
                )
            )
            .query("non_extreme_values == True")
            .drop(["non_extreme_values"], axis=1)
        )

    @staticmethod
    def filter_countries(
        df: pd.DataFrame, countries: List[str]
    ) -> pd.DataFrame:
        """
        Filters the DataFrame for specific countries.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing location data.
        countries : list of str
            The list of countries to filter for.

        Returns
        -------
        pd.DataFrame
            The filtered DataFrame containing only rows from the specified countries.
        """

        def check_country(country):
            # Print the comparison for debugging
            print(f"Country string: {country}")
            for str_ in countries:
                print(f"Checking if '{str_}' matches '{country}'")
                if str_ == country:
                    return True
            return False

        df["filtered_country"] = df.country.apply(check_country)
        print(
            f"After applying country filter:\n{df[['country', 'filtered_country']]}"
        )

        return df.query("filtered_country == True").drop(
            ["filtered_country"], axis=1
        )

    @staticmethod
    def filter_cities(df: pd.DataFrame, cities: List[str]) -> pd.DataFrame:
        """
        Filters the DataFrame for specific cities.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing location data.
        cities : list of str
            The list of cities to filter for.

        Returns
        -------
        pd.DataFrame
            The filtered DataFrame containing only rows from the specified cities.
        """
        return (
            df.assign(
                filtered_cities=(
                    df.city.apply(
                        lambda city: any(
                            str_ in city[1:-1].split(",") for str_ in cities
                        )
                    )
                )
            )
            .query("filtered_cities == True")
            .drop(["filtered_cities"], axis=1)
        )
