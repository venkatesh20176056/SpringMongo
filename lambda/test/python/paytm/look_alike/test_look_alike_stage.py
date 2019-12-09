import unittest
import json

from paytm.look_alike.look_alike_stage import remove_look_alike_segments, remove_look_alike_segments_old_cma


class TestLookAlikeStage(unittest.TestCase):

    def test_remove_look_alike_segments_old_cma(self):
        scheduled_campaign = json.loads(_scheduled_campaign_old_cma_str)
        campaign_details = json.loads(_campaign_details_str)
        expected = json.loads(_expected_campaign_old_cma_str)
        campaign = remove_look_alike_segments_old_cma(scheduled_campaign, campaign_details)
        self.assertEqual(campaign, expected)

    def test_interop_old_data_new_remove(self):
        scheduled_campaign = json.loads(_scheduled_campaign_old_cma_str)
        campaign = remove_look_alike_segments(scheduled_campaign)
        self.assertEqual(campaign, scheduled_campaign)

    def test_interop_new_data_old_remove(self):
        scheduled_campaign = json.loads(_scheduled_campaign_new_cma_str)
        campaign_details = json.loads(_campaign_details_str)
        campaign = remove_look_alike_segments_old_cma(scheduled_campaign, campaign_details)
        self.assertEqual(campaign, scheduled_campaign)

    def test_remove_look_alike_segments_new_cma(self):
        scheduled_campaign = json.loads(_scheduled_campaign_new_cma_str)
        expected = json.loads(_expected_campaign_new_cma_str)
        campaign = remove_look_alike_segments(scheduled_campaign)
        self.assertEqual(campaign, expected)


_scheduled_campaign_old_cma_str = '''
{
  "id": 544,
  "inclusion": {
    "uploads": [
      260
    ],
    "campaigns": [],
    "rules": [
      {
        "op": "OR",
        "type": "bool",
        "operands": [
          {
            "op": "AND",
            "type": "bool",
            "operands": [
              {
                "op": "=",
                "operands": [
                  "PAYTM_total_transaction_count_90_days",
                  "0"
                ],
                "dataType": "number"
              },
              {
                "op": ">",
                "operands": [
                  "isMerchant",
                  "0"
                ],
                "dataType": "number"
              }
            ]
          }
        ]
      }
    ],
    "csvs": [
      {
        "url": "https://map-cma-upload-staging.s3.ap-south-1.amazonaws.com/b92cad58-35af-4f72-9805-017a9ee5ee77.csv",
        "size": 100000
      }
    ]
  }
}
'''


_campaign_details_str = '''
{
  "id": 544,
  "includedSegments": [
    {
      "id": 259,
      "name": "NoTransactionsMerchant",
      "type": "RULE_BASED",
      "description": "Group with no transactions in the last 90 days",
      "csvUrl": ""
    },
    {
      "id": 260,
      "name": "lookalike_for_544_2632ae38-d3ab-4e3e-9c21-3f0f8f74d7d4",
      "type": "CSV_BASED",
      "description": "synthetic-lookalike-segment",
      "csvUrl": "https://map-cma-upload-staging.s3.ap-south-1.amazonaws.com/b92cad58-35af-4f72-9805-017a9ee5ee77.csv"
    }
  ]
}
'''


_expected_campaign_old_cma_str = '''
{
  "id": 544,
  "inclusion": {
    "uploads": [],
    "campaigns": [],
    "rules": [
      {
        "op": "OR",
        "type": "bool",
        "operands": [
          {
            "op": "AND",
            "type": "bool",
            "operands": [
              {
                "op": "=",
                "operands": [
                  "PAYTM_total_transaction_count_90_days",
                  "0"
                ],
                "dataType": "number"
              },
              {
                "op": ">",
                "operands": [
                  "isMerchant",
                  "0"
                ],
                "dataType": "number"
              }
            ]
          }
        ]
      }
    ],
    "csvs": []
  }
}
'''


_scheduled_campaign_new_cma_str = '''
{
  "id": 544,
  "inclusion": {
    "segments": [
      {
        "id": 260,
        "type": "CSV_BASED",
        "includedRules": [],
        "excludedRules": [],
        "description": "synthetic-lookalike-segment",
        "visible": true,
        "csvUrl": "https://map-cma-upload-staging.s3.ap-south-1.amazonaws.com/b92cad58-35af-4f72-9805-017a9ee5ee77.csv"
      },
      {
        "id": 259,
        "type": "RULE_BASED",
        "includedRules": [
          {
            "createdAt": "2018-09-07T21:08:05",
            "createdBy": "aleksey.nikiforov@paytm.com",
            "updatedAt": null,
            "updatedBy": null,
            "id": 198,
            "expression": {
              "op": "OR",
              "type": "bool",
              "operands": [
                {
                  "op": "AND",
                  "type": "bool",
                  "operands": [
                    {
                      "op": "=",
                      "operands": [
                        "PAYTM_total_transaction_count_90_days",
                        "0"
                      ],
                      "dataType": "number"
                    },
                    {
                      "op": ">",
                      "operands": [
                        "isMerchant",
                        "0"
                      ],
                      "dataType": "number"
                    }
                  ]
                }
              ]
            }
          }
        ],
        "excludedRules": [],
        "description": "Group with no transactions in the last 90 days",
        "visible": true,
        "csvUrl": ""
      }
    ]
  }
}
'''


_expected_campaign_new_cma_str = '''
{
  "id": 544,
  "inclusion": {
    "segments": [
      {
        "id": 259,
        "type": "RULE_BASED",
        "includedRules": [
          {
            "createdAt": "2018-09-07T21:08:05",
            "createdBy": "aleksey.nikiforov@paytm.com",
            "updatedAt": null,
            "updatedBy": null,
            "id": 198,
            "expression": {
              "op": "OR",
              "type": "bool",
              "operands": [
                {
                  "op": "AND",
                  "type": "bool",
                  "operands": [
                    {
                      "op": "=",
                      "operands": [
                        "PAYTM_total_transaction_count_90_days",
                        "0"
                      ],
                      "dataType": "number"
                    },
                    {
                      "op": ">",
                      "operands": [
                        "isMerchant",
                        "0"
                      ],
                      "dataType": "number"
                    }
                  ]
                }
              ]
            }
          }
        ],
        "excludedRules": [],
        "description": "Group with no transactions in the last 90 days",
        "visible": true,
        "csvUrl": ""
      }
    ]
  }
}
'''
