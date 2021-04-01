import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
from datetime import date
import sqlite3
import smtplib, ssl

def check_if_valid_data(df: pd.DataFrame) -> bool:
        if df.empty:
                print("No major holidays listed in the database for today.")
                return False

        if pd.Series(df["holiday_name"]).is_unique:
                pass
        else:
                raise Exception("Primary Key Check is violated")

        if df.isnull().values.any():
                raise Exception("Null value found")

        returned_dates = df["holiday_date"].tolist()
        for returned_date in returned_dates:
                if datetime.date.today().strftime("%Y-%m-%d") != returned_date:
                        raise Exception("At least one of the holidays returned is not celebrated on today's date")

        return True


def run_calenderific_etl():

        database_location = "sqlite:///todays_holidays.sqlite"
        token = "Your API token"
        country = "us"
        year = "2021"

        day = datetime.date.today().strftime('%d')
        month = datetime.date.today().strftime('%m')
        today = datetime.date.today().strftime("%Y-%m-%d")

        r = requests.get(f"https://calendarific.com/api/v2/holidays?api_key={token}"
                                         f"&country={country}&year={year}&month={month}&day={day}")

        data = r.json()

        holiday_names = []
        descriptions = []
        holiday_dates = []

        for date in data["response"]["holidays"]:
                holiday_names.append(date["name"])
                descriptions.append(date["description"])
                holiday_dates.append(date["date"]["iso"])

        holiday_dict = {

                "holiday_name": holiday_names,
                "description": descriptions,
                "holiday_date": holiday_dates
        }

        holiday_df = pd.DataFrame(holiday_dict, columns=["holiday_name", "description", "holiday_date"])

        print(holiday_df)

        # Validate
        if check_if_valid_data(holiday_df):
                print("Data valid, proceed to load stage")

        # Load
        engine = sqlalchemy.create_engine(database_location)
        conn = sqlite3.connect("todays_holidays.sqlite")
        cursor = conn.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS todays_holidays(
                holiday_name VARCHAR(300),
                description BLOB,
                holiday_date CHAR(10),
                CONSTRAINT primary_key_constraint PRIMARY KEY (holiday_name)
        )
        """
        cursor.execute(sql_query)
        print("Opened database successfully")

        #Email
        port = 465
        smtp_server = "smtp.gmail.com"
        sender_email = "your email address"
        receiver_email = "recipient email address"
        results = cursor.execute("SELECT * FROM todays_holidays WHERE holiday_date=(?)", (today,))
        password = input("Type your password and press enter: ")
        message = "Test message"
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login("Your e-mail address"Y, password)
            server.sendmail(sender_email, receiver_email, message)

        try:
                holiday_df.to_sql("todays_holidays", engine, index=False, if_exists='append')
        except:
                print("Data already exists in the database")

        conn.close()
        print("Closed database successfully")
