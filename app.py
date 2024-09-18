from chalice import Chalice
import boto3
from boto3.dynamodb.conditions import Key

app = Chalice(app_name='src')


@app.route('/')
def index():
    return {'hello': 'world'}


@app.route('/customers')
def get_customers():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('customer-data-int')
    response = table.scan()
    return response.get("Items")


@app.route('/customers/{customer_id}')
def get_customer(customer_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('customer-data-int')
    response = table.query(
        KeyConditionExpression = Key("customer_id").eq(customer_id)
    )

    if response.get("Count") == 0:
        return f"Not found customer with {customer_id}", 404
    
    return response.get('Items')[0]


@app.route('/customers/{customer_id}', methods=["PATCH"])
def update_customer(customer_id):
    request = app.current_request
    request_body = request.json_body
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('customer-data-int')

    response = table.query(
        KeyConditionExpression = Key("customer_id").eq(customer_id)
    )

    if response.get("Count") == 0:
        return f"customer not found with id {customer_id}", 404

    update_expression = "set " +  ",".join([f"{key}=:{key}" for key in request_body])
    expression_attribute_values = {f":{key}": value  for key, value in request_body}

    response = table.update_item(
        Key={"customer_id": customer_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="UPDATED_NEW",
    )
    
    return {"message": "success"}


@app.route('/customers', methods=["POST"])
def add_customer():
    request = app.current_request
    request_body = request.json_body
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('customer-data-int')

    if request_body.get("customer_id") is None:
        return "Customer id required", 400

    table.put_item(Item=request_body)
    
    return {"message": "success"}


@app.route('/customers/{customer_id}', methods=["DELETE"])
def delete_customer(customer_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('customer-data-int')

    table.delete_item(Key={"customer_id": customer_id})
    
    return {"message": "success"}
