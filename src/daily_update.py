# Imports
import requests
import pandas as pd
import datetime
import boto3
import private
import logging
from yahoo_fin import stock_info as si


def weekly_update():
    """
    Pulls stock data from Yahoo Finance into Dataframe
    and sends email via Amazon SES
    """
    # -----------------------------------
    # Constants
    # -----------------------------------
    SP_500_TICKERS = si.tickers_sp500()
    BASE_URL_1 = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
    BASE_URL_2 = (
        "?formatted=true&crumb=swg7qs5y9UP&lang=en-US&region=US&"
        "modules=defaultKeyStatistics,recommendationTrend,summaryDetail,"
        "financialData&"
        "corsDomain=finance.yahoo.com"
    )
    logger = logging.getLogger()

    # -----------------------------------
    # Get first set of S&P 500 data
    # -----------------------------------
    financials = []
    for ticker in SP_500_TICKERS:
        logger.info(f"Getting data for ticker, {ticker}")

        # Get url and make request
        url = BASE_URL_1 + ticker + BASE_URL_2
        response = requests.get(url)
        response_json = response.json()

        # returns true if status_code less than 400
        if not response.ok:
            logger.error(f"Error for ticker, {ticker}")
            row = [ticker, None, None, None, None, None, None, None, None]
        try:
            results_json = response_json["quoteSummary"]["result"][0]
            price = results_json["financialData"]["currentPrice"]["fmt"]
            shares_outstanding = results_json["defaultKeyStatistics"][
                "sharesOutstanding"
            ]["fmt"]
            market_cap = results_json["summaryDetail"]["marketCap"]["fmt"]
            total_cash = results_json["financialData"]["totalCash"]["fmt"]
            total_debt = results_json["financialData"]["totalDebt"]["fmt"]
            total_revenue = results_json["financialData"]["totalRevenue"]["fmt"]
            enterprise_value = results_json["defaultKeyStatistics"]["enterpriseValue"][
                "fmt"
            ]
            forward_pe = results_json["defaultKeyStatistics"]["forwardPE"]["fmt"]

            row = [
                ticker,
                price,
                shares_outstanding,
                market_cap,
                total_cash,
                total_debt,
                total_revenue,
                enterprise_value,
                forward_pe,
            ]

        except Exception as e:
            logger.error(f"Error for ticker, {ticker}")
            row = [ticker, None, None, None, None, None, None, None, None]

        financials.append(row)

    # Create pandas dataframe and insert data
    my_columns = [
        "Company",
        "Price",
        "S/O",
        "Market Cap",
        "Cash",
        "Debt",
        "Revenue",
        "EV",
        "Forward PE",
    ]
    df = pd.DataFrame(financials, columns=my_columns)
    df = df.set_index("Company")

    ratings = []

    # -----------------------------------
    # Get analyst recommendations
    # -----------------------------------
    for ticker in SP_500_TICKERS:
        logger.info(f"Getting ratings for ticker, {ticker}")

        # Get url and make request
        url = BASE_URL_1 + ticker + BASE_URL_2
        response = requests.get(url)
        response_json = response.json()

        try:
            # returns true if status_code less than 400
            if not response.ok:
                logger.error(f"Error for ticker, {ticker}")
                row = [ticker, None, None]

            # Parse Response
            results_json = response_json["quoteSummary"]["result"][0]
            current_month = results_json["recommendationTrend"]["trend"][0]

            # Get ratings
            strong_buy = current_month["strongBuy"]
            buy = current_month["buy"]
            hold = current_month["hold"]
            sell = current_month["sell"]
            strong_sell = current_month["strongSell"]

            # Calculate average
            total_ratings = (
                current_month["strongBuy"]
                + current_month["buy"]
                + current_month["hold"]
                + current_month["sell"]
                + current_month["strongSell"]
            )
            total_score = (
                current_month["strongBuy"] * 1
                + current_month["buy"] * 2
                + current_month["hold"] * 3
                + current_month["sell"] * 4
                + current_month["strongSell"] * 5
            )
            average_score = round(total_score / total_ratings, 2)

            row = [ticker, average_score, total_ratings]
            ratings.append(row)

        except Exception as e:
            logger.error(f"Error for ticker, {ticker}")
            row = [ticker, None, None]

    # Create dataframe and insert data
    my_columns = ["Company", "Average Score", "Total Ratings"]
    df2 = pd.DataFrame(ratings, columns=my_columns)
    df2 = df2.set_index("Company")

    df_new = df.copy()
    df_new["Average Score"] = df2["Average Score"]
    df_new["Total Ratings"] = df2["Total Ratings"]

    # -----------------------------------
    # Clean and copy df
    # -----------------------------------
    df_clean = df_new.dropna()
    df_final = df_clean.copy()

    # Get personal list of tickers from S&P500
    my_tickers = private.MY_TICKERS
    df_my_tickers = df_final.transpose()[my_tickers]

    # -----------------------------------
    # Send email w/ dataframe
    # -----------------------------------
    client = boto3.client(
        "ses",
        aws_access_key_id=private.AWS_ACCESS_KEY,
        aws_secret_access_key=private.AWS_SECRET_KEY,
        region_name=private.AWS_REGION,
    )

    # Save files to csv
    now = datetime.datetime.now()
    subject = f"Daily stock data {now.month}/{now.day}/{now.year}"
    html_content = df_my_tickers.to_html()

    try:
        logger.info("Sending email")
        response = client.send_email(
            Source=private.FROM_EMAIL,
            Destination={"ToAddresses": [private.TO_EMAIL],},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Html": {"Data": html_content, "Charset": "UTF-8"}},
            },
        )
    except Exception as ex:
        logger.error(str(ex), exc_info=ex)
    else:
        logger.info("Message sent")


if __name__ == "__main__":
    weekly_update()
