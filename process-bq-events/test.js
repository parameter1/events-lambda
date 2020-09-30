const { handler } = require('./index');

const Records = [
  {
    "_id": "kDK1uP17abQkfcFy-PWZ5",
    "ts": 1601405069731,
    "slug": "acbm",
    "host": "0.0.0.0:9716",
    "act": "View",
    "cat": "Website Section",
    "ent": "base.acbm-fcp.website-section*54539",
    "vis": "7SFGIWm0FYG8eUyuG92_c",
    "idt": "omeda.hcl.customer*6334G3807412J6A~encoded",
    "ip": "24.197.191.115",
    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "version": "2",
    "entity": {
        "id": "base.acbm-fcp.website-section*54539",
        "name": "Trucks > Trucks & Accessories > Pickup Trucks & Vans",
        "refs": {}
    }
  },
  {
    "_id": "IioS0Q1s50NifDtRktRFz",
    "ts": 1601401927619,
    "slug": "acbm",
    "host": "0.0.0.0:9716",
    "act": "View",
    "cat": "Content",
    "ent": "base.acbm-fcp.content-blog*21195471",
    "vis": "7SFGIWm0FYG8eUyuG92_c",
    "idt": "omeda.hcl.customer*6334G3807412J6A~encoded",
    "ip": "24.197.191.115",
    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "version": "2",
    "entity": {
      "id": "base.acbm-fcp.content-blog*21195471",
      "name": "House Approves Continuing Resolution that Funds Highway Bill through 2021",
      "props": {
        "type": "blog",
        "published": 1600975184000
      },
      "refs": {
        "primarySection": {
          "id": "base.acbm-fcp.website-section*54551",
          "name": "Blogs > Construction Grade",
          "props": {
            "alias": "blogs/construction-grade"
          },
          "refs": {}
        },
        "company": null,
        "authors": [
          {
            "id": "base.acbm-fcp.content-contact*10209849",
            "name": "Larry Stewart",
            "refs": {}
          },
          {
            "id": "base.acbm-fcp.content-contact*10209848",
            "name": "Jacob Bare",
            "refs": {}
          }
        ],
        "createdBy": {
          "id": "base.acbm-fcp.user*53ca8d6b1784f8066eb2c94f",
          "name": "lstewart",
          "props": {
            "firstName": "Larry",
            "lastName": "Stewart"
          },
          "refs": {}
        }
      }
    }
  },
  {
    "_id": "Ns5oWH9a2b29vnkMbg3pz",
    "ts": 1601402506652,
    "slug": "acbm",
    "host": "0.0.0.0:9716",
    "act": "View",
    "cat": "Content",
    "ent": "base.acbm-fcp.content-article*21195876",
    "vis": "7SFGIWm0FYG8eUyuG92_c",
    "idt": "omeda.hcl.customer*6334G3807412J6A~encoded",
    "ip": "24.197.191.115",
    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "version": "2",
    "entity": {
      "id": "base.acbm-fcp.content-article*21195876",
      "name": "2021 Chevrolet Silverado 1500  Adds Multi-Flex Tailgate and Trailering Enhancements",
      "props": {
        "type": "article",
        "published": 1601327630000
      },
      "refs": {
        "primarySection": {
          "id": "base.acbm-fcp.website-section*54539",
          "name": "Trucks > Trucks & Accessories > Pickup Trucks & Vans",
          "props": {
            "alias": "trucks/trucks-accessories/pickup-trucks-vans"
          },
          "refs": {}
        },
        "company": {
          "id": "base.acbm-fcp.content-company*10842418",
          "name": "General Motors Company",
          "refs": {}
        },
        "authors": [],
        "createdBy": {
          "id": "base.acbm-fcp.user*53ca8daa1784f8066eb2c95d",
          "name": "cbennink",
          "props": {
            "firstName": "Curt",
            "lastName": "Bennink"
          },
          "refs": {}
        }
      }
    }
  }
].map((v) => ({ body: JSON.stringify(v) }));

handler({ Records }).catch((e) => setImmediate(() => { throw e; }));
