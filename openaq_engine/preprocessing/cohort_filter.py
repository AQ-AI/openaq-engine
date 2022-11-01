from itertools import chain
import pandas as pd
from src.utils.utils import get_data


class Filter:
    @staticmethod
    def filter_countries(df: pd.DataFrame, countries: List[str]) -> pd.DataFrame:
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
                            str_ in country[1:-1].split(",") for str_ in countries
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