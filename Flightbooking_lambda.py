import json
import datetime
import time
import os
import dateutil.parser
import logging
import boto3
import uuid

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

dyn_client = boto3.client('dynamodb')
TABLE_NAME = "FlightsBooking"

BUCKET_NAME = "flightbookings1209"
s3_client = boto3.client('s3')

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:15712942881:NotifyAdmin"


def alert_admin_using_sns(message, subject):
    sns_client.publish(TopicArn=SNS_TOPIC_ARN,
                       Message=message,
                       Subject=subject)


def store_details_to_s3(bucket, key, data):
    s3_client.put_object(Body=data, Bucket=bucket, Key=key)


def dispatch(intent_request):
    logger.debug(
        'dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'BookFlight':
        return take_flightbooking(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return dispatch(event)


# --- Helper Functions ---

def safe_int(n):
    """
    Safely convert n value to int.
    """
    if n is not None:
        return int(n)
    return n


def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# -- DynamoDB Save
def save_flight(flightOut, flightBack, fromAirportcode, toAirportcode, seatTypes, numberofCheckedBags, passengerFNAME,
                passengerLNAME, passengerDOB, passengerEmailAddress, passengerPhone):
    id = str(uuid.uuid4())

    flightBookings = json.dumps({
        'flightOut': flightOut,
        'flightBack': flightBack,
        'fromAirportcode': fromAirportcode,
        'toAirportcode': toAirportcode,
        'seatTypes': seatTypes,
        'numberofCheckedBags': numberofCheckedBags,
        'passengerFNAME': passengerFNAME,
        'passengerLNAME': passengerLNAME,
        'passengerDOB': passengerDOB,
        'passengerEmailAddress': passengerEmailAddress,
        'passengerPhone': str(passengerPhone),
    })

    data = dyn_client.put_item(
        TableName=TABLE_NAME,
        Item={
            'id': {
                'S': id
            },
            'flightOut': {
                'S': flightOut
            },
            'flightBack': {
                'S': flightBack
            },
            'fromAirportcode': {
                'S': fromAirportcode
            },
            'toAirportcode': {
                'S': toAirportcode
            },
            'seatTypes': {
                'S': seatTypes
            },
            'numberofCheckedBags': {
                'N': numberofCheckedBags
            },
            'passengerFNAME': {
                'S': passengerFNAME
            },
            'passengerLNAME': {
                'S': passengerLNAME
            },
            'passengerDOB': {
                'S': passengerDOB
            },
            'passengerEmailAddress': {
                'S': passengerEmailAddress
            },
            'passengerPhone': {
                'N': str(passengerPhone)
            }

        }
    )
    store_details_to_s3(BUCKET_NAME, "flight/{}.txt".format(id), flightBookings)


# --- Validation Functions ---

def isvalid_seatType(seatType):
    seatTypes = ['economy', 'economy plus', 'business', 'first class']
    return seatType.lower() in seatTypes


def validate_bookings(slots):
    flightOut = try_ex(lambda: slots['FlightOut'])
    flightBack = try_ex(lambda: slots['FlightBack'])
    fromAirportcode = try_ex(lambda: slots['FromAirportCode'])
    toAirportcode = try_ex(lambda: slots['ToAirportCode'])
    seatType = try_ex(lambda: slots['SeatTypes'])
    numberofCheckedBags = safe_int(try_ex(lambda: slots['NumberCheckedBags']))
    passengerFNAME = try_ex(lambda: slots['PassengerFirstName'])
    passengerLNAME = try_ex(lambda: slots['PassengerLastName'])
    passengerDOB = try_ex(lambda: slots['PassengerDOB'])
    passengerEmailAddress = try_ex(lambda: slots['PassengerEmailAddress'])
    passengerPhone = try_ex(lambda: slots['PassengerPhone'])

    if seatType and not isvalid_seatType(seatType):
        return build_validation_result(
            False,
            'SeatTypes',
            'I did not recognize that Class Seat type.  Which class seat will be selected for this flight? (Economy, Economy Plus, Business, First) '
        )

    return {'isValid': True}


def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def take_flightbooking(intent_request):
    slots = intent_request['currentIntent']['slots']
    flightOut = slots['FlightOut']
    flightBack = slots['FlightBack']
    fromAirportcode = slots['FromAirportCode']
    toAirportcode = slots['ToAirportCode']
    seatType = slots['SeatTypes']
    numberofCheckedBags = slots['NumberofCheckedBags']
    passengerFNAME = slots['PassengerFirstName']
    passengerLNAME = slots['PassengerLastName']
    passengerDOB = slots['PassengerDOB']
    passengerEmailAddress = slots['PassengerEmailAddress']
    passengerPhone = slots['PassengerPhone']

    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

    logger.debug(intent_request['invocationSource'])

    if intent_request['invocationSource'] == 'DialogCodeHook':

        validation_result = validate_bookings(intent_request['currentIntent']['slots'])
        logger.debug(validation_result)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )
        return delegate(session_attributes, intent_request['currentIntent']['slots'])

    save_flight(flightOut, flightBack, fromAirportcode, toAirportcode, seatType, numberofCheckedBags, passengerFNAME,
                passengerLNAME, passengerDOB, passengerEmailAddress, passengerPhone)

    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Thanks {} {}, we have recorded your booking request'.format(passengerFNAME, passengerLNAME)
        }
    )


