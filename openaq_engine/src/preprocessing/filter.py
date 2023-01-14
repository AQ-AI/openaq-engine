from typing import List

import pandas as pd


class Filter:
    @staticmethod
    def filter_pollutant(
        df: pd.DataFrame, pollutant_to_predict: str
    ) -> pd.DataFrame:
        """
        Filter for rows selected pollutant

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with selected `pollutant`
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
        Filter for rows selected pollutant

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with no empty `coordinates`
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
        Filter out rows which are non null

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with 0 values
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
        Filter out rows which contain extremely high pm25 values

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with extreme values removed
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
        Filter for countries

        Parameters
        ----------
        df : pd.DataFrame
        countries: list with `countries`
        """

        return (
            df.assign(
                filtered_country=(
                    df.country.apply(
                        lambda country: any(
                            str_ in country[1:-1].split(",")
                            for str_ in countries
                        )
                    )
                )
            )
            .query("filtered_country == True")
            .drop(["filtered_country"], axis=1)
        )

    @staticmethod
    def filter_cities(df: pd.DataFrame, cities: List[str]) -> pd.DataFrame:
        """
        Filter for cities

        Parameters
        ----------
        df : pd.DataFrame
        cities: list with `cities`
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
