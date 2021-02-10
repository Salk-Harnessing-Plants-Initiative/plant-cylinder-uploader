import json
import boto3

d = {
	"qr_code" : "GI-wNbpbOPEMsMHRqKMsL",
	"upload_device+_id" : "testing"
}

client = boto3.client('lambda')
response = client.invoke(
    FunctionName='arn:aws:lambda:us-west-2:295111184710:function:preflight-cylinder-image-upload',
    LogType='None',
    Payload=json.dumps(d)
)
payload = json.loads(response['Payload'].read())
print(payload['qr_code_valid'])
