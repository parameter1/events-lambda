const { handler } = require('./index');

const Records = [
  {
    "id": "base.acbm-fcp.content-product*21195140",
    "name": "TB257FR Compact Hydraulic Excavator",
    "props": {
      "bar": "foo"
    },
    "refs": {
      "company": null,
      "primarySection": {
        "id": "base.acbm-fcp.website-section*54365",
        "name": "Equipment > Earthmoving - Compact > Mini Excavators",
        "refs": {
          "site": {
            "id": "base.acbm-fcp.product-site*53ca8d671784f8066eb2c949",
            "name": "For Construction Pros"
          }
        }
      },
      "sections": [
        {
          "id": "base.acbm-fcp.website-section*54365",
          "refs": {}
        }
      ],
      "taxonomy": []
    }
  },
  {
    "id": "base.acbm-fcp.content-product*10071828"
  }
].map((v) => ({ body: JSON.stringify(v) }));

handler({ Records }).catch((e) => setImmediate(() => { throw e; }));
