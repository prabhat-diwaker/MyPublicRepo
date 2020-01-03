import exceptions
import commands
from google.cloud import storage
import logging


def add_bucket_iam_member(bucket_name,member,role):
    #bucket_name = '03bc6fd25fc00f4af20ce60226f7a559a20c0be37fd3a36c9e2e5b924b4bd0'
    #role = 'roles/storage.objectViewer'
    #member = 'serviceAccount:bfdms-sa-mer-fdse-prod-sec@wmt-ae72e7270dedc6536a95fff04f.iam.gserviceaccount.com'
    try :
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        policy = bucket.get_iam_policy()

        policy[role].add(member)

        bucket.set_iam_policy(policy)

        print ('Added {} with role {} to {}.'.format(member, role, bucket_name))

    except Exception as e:
        print e


if __name__ == '__main__':
    binpath = '/Users/p0d00mp/Downloads/gsdk/google-cloud-sdk/bin'

    SA1 = 'serviceAccount:bfdms-sa-mer-fdse-prod-sec@wmt-ae72e7270dedc6536a95fff04f.iam.gserviceaccount.com'
    SA2 = 'serviceAccount:bfdms-sa-mer-fdse-prod@wmt-ae72e7270dedc6536a95fff04f.iam.gserviceaccount.com'

    roles = ['roles/storage.objectViewer', 'roles/storage.legacyObjectReader', 'roles/storage.legacyBucketReader']

    try :
        #get list of all the buckets in the project (wmt-gdap-dl-sec-merch-prod as authenticated in SDK )
        cmd = 'gsutil ls'
        status, output = commands.getstatusoutput(cmd)
        buckets=output.split()
        list_of_bucket=buckets[70:]
        print "List_of_buckets to be processed:\n"
        print list_of_bucket
        #list_of_bucket=['gs://prabhat_test']
        for bucket in list_of_bucket:
            bucket_name = bucket.split('/')[2]
            for role in roles:
                add_bucket_iam_member(bucket_name,SA1,role)
                add_bucket_iam_member(bucket_name,SA2,role)

    except Exception as e:
        print e

