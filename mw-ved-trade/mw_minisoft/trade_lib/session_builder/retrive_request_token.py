import time
from datetime import date
from datetime import datetime
from urllib.parse import urlsplit, parse_qs
import numpy as np
import pyotp as pyotp
from fyers_apiv3 import fyersModel
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import requests
import json
from mw_minisoft.trade_logger.logger import cus_logger


def obtain_access_token(user_record, firefox_driver_path, session):
    """
        This is the program's very first step; it will obtain the request token based on the
        username, password and toptp token.

        login_url = "http://13.127.40.204//api/v2/generate-authcode?client_id=44RM7YTQWY-100&redirect_uri=http://localhost:8080/&esponse_type=code&state=None"
    """

    login_url = "https://api.fyers.in/api/v2/generate-authcode?" \
                + "client_id=" + user_record.api_key \
                + "&redirect_uri=" + session.redirect_uri \
                + "&response_type=" \
                + session.response_type \
                + "&state=None"
    options = Options()
    # options.add_argument("--headless")
    options.profile = webdriver.FirefoxProfile(
        "C:\\Users\\sriram\\AppData\\Roaming\\Mozilla\\Firefox\\Profiles\\3j33tq1.default")
    driver = webdriver.Firefox(options=options)

    try:
        driver.get(login_url)
        wait = WebDriverWait(driver, 20)
        # Find User id field and set user id, and password
        time.sleep(3)
        wait.until(EC.element_to_be_clickable((By.XPATH, "// *[ @ id = 'login_client_id']"))).click()
        time.sleep(3)
        wait.until(EC.presence_of_element_located((By.XPATH, "//input[@id='fy_client_id']"))) \
            .send_keys(user_record.user_id)
        time.sleep(3)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@id='clientIdSubmit']"))).click()
        time.sleep(3)
        time_otp = pyotp.TOTP(user_record.totp).now()
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='otp-container']/input"))) \
            .send_keys(time_otp)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@id='confirmOtpSubmit']"))).click()
        time.sleep(3)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='pin-container']/input"))) \
            .send_keys(user_record.login_pin)
        wait.until(EC.element_to_be_clickable((By.XPATH, "// *[@id ='verifyPinSubmit']"))).click()
        time.sleep(60)
        wait.until(EC.url_contains('code=200'))

    except Exception as exception:
        cus_logger.error("Obtaining Access Token for the user(%s) had been failed, Error message %s",
                         user_record.user_id, exception)
        driver.quit()

    request_token = parse_qs(urlsplit(driver.current_url).query)['auth_code'][0]
    print(request_token)
    driver.quit()
    return request_token


def generate_auth_code(session):
    profile_path = "C:\\Users\\sriram\\AppData\\Roaming\\Mozilla\\Firefox\\Profiles\\wkycbwh7.default-release" \
                   "-1666779937972 "
    login_url = session.generate_authcode()
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    options.profile = webdriver.FirefoxProfile(profile_path)
    driver = webdriver.Firefox(options=options)
    driver.get(login_url)
    wait = WebDriverWait(login_url, 20)
    request_token = parse_qs(urlsplit(driver.current_url).query)['auth_code'][0]
    return request_token


def create_user_session(user_record, firefox_driver_path):
    """
    This code will generate the accessToken from the api, later will be used to generate the user session
    """
    cus_logger.info("crating the new user session")
    try:
        if (int(date.today().day)) != int(user_record.day):
            session = fyersModel.SessionModel(client_id=user_record["api_key"],
                                              secret_key=user_record["api_secret"],
                                              redirect_uri='http://3.111.132.88/',
                                              response_type="code", grant_type="authorization_code")

            # https://api-t1.fyers.in/api/v3/generate-authcode?client_id=OIKVPCOSEV-100&redirect_uri=http%3A%2F%2F3.111.132.88%2F&response_type=code&state=None
            # response = session.generate_authcode()
            # request_token = obtain_access_token(user_record, firefox_driver_path, session)
            # generate_auth_code(session)

            auth_token_date = datetime.strptime(user_record.auth_token_date, '%Y-%m-%d').date()
            auth_token_date_diff = (-(auth_token_date - date.today()).days)

            if auth_token_date_diff > 14:
                session.set_token(user_record.auth_code)
                user_access_refresh_token = session.generate_token()
                user_record = update_user_record(user_record, user_record.auth_code,
                                                 user_access_refresh_token['refresh_token'],
                                                 user_access_refresh_token['access_token'],
                                                 date.today())
            else:
                response = response_refresh_token(user_record)
                user_record = update_user_record(user_record, user_record.auth_code,
                                                 user_record.refresh_token,
                                                 response.json()['access_token'],
                                                 user_record.auth_token_date)

            kite_connect = generate_user_session(user_record)
        else:
            kite_connect = generate_user_session(user_record)

        return kite_connect, user_record

    except Exception as exception:
        cus_logger.error("creation of user(%s) session  had been failed; Error message %s", user_record.user_id,
                         exception)


def response_refresh_token(user_record):
    url = "https://api-t1.fyers.in/api/v3/validate-refresh-token"
    payload = json.dumps({
        "grant_type": "refresh_token",
        #"appIdHash": user_record.api_key + ":" + user_record.api_secret,
        "appIdHash": '3d8b28cb1e2b05958ea2bacd834cbda6bade551bd920300de9a9d317f26ebbd7',
        "refresh_token": user_record.refresh_token,
        "pin": user_record.login_pin
    })
    headers = {
        'Content-Type': 'application/json',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response


def update_user_record(user_record, auth_code, refresh_token, access_token, auth_token_date):
    """
        user record will be updated over here
        :rtype: excel data
    """
    user_record['auth_code'] = auth_code
    user_record['day'] = int(date.today().day)
    user_record['refresh_token'] = refresh_token
    user_record['access_token'] = access_token
    user_record['auth_token_date'] = auth_token_date
    cus_logger.info("updating the user_record")

    return user_record


def generate_user_session(user_record):
    """
        The USER session will be generated by using existing API keys in the system.
    """
    fyers = fyersModel.FyersModel(client_id=user_record.api_key, token=user_record.access_token, log_path="")
    cus_logger.info("A new user session has been created; the session will be returned.")

    return fyers




