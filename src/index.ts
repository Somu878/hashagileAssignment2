import { Client } from "@elastic/elasticsearch";
import fs from "fs";
import csv from "csv-parser";

//Employee type
interface Employee {
  EmployeeID: number;
  EmployeeName: string;
  Age: number;
  Gender: string;
  Department: string;
  Position: string;
  Salary: number;
}

//elastic search cliemt
const client = new Client({
  node: "http://localhost:9200",
  auth: {
    username: "elastic",
    password: "h9vjmdT-63XbJU_A3ajr",
  },
});

//create index function "Employee"
async function createIndex(): Promise<void> {
  try {
    const indexExists = await client.indices.exists({ index: "employee2" });
    //if no employee index not existed, creates a new index
    if (!indexExists) {
      await client.indices.create({
        index: "employee2",
        body: {
          mappings: {
            properties: {
              EmployeeID: { type: "integer" },
              EmployeeName: { type: "text" },
              Age: { type: "integer" },
              Gender: { type: "keyword" },
              Department: { type: "text" },
              Position: { type: "text" },
              Salary: { type: "float" },
            },
          },
        },
      });

      console.log('Index named "employee" is created');
    } else {
      console.log("Index named 'employee' already exists");
    }
  } catch (error) {
    console.log("Unable to create the index", error);
  }
}

async function indexData(): Promise<void> {
  const employees: Array<{ index: { _index: string } } | Employee> = [];
  //D:\workspace\elasticSearch-Assignment\Employee Sample Data 1.csv
  // Read and parse the CSV file
  fs.createReadStream("./employeeData.csv")
    .pipe(csv())
    .on("data", (row: any) => {
      employees.push(
        { index: { _index: "employee" } },
        {
          EmployeeID: parseInt(row.EmployeeID, 10),
          EmployeeName: row.EmployeeName,
          Age: parseInt(row.Age, 10),
          Gender: row.Gender,
          Department: row.Department,
          Position: row.Position,
          Salary: parseFloat(row.Salary),
        }
      );
    })
    .on("end", async () => {
      try {
        // Bulk insert employee data into elasticsearch
        const { body } = (await client.bulk({
          refresh: true,
          body: employees,
        })) as any;

        if (body.errors) {
          console.error("Bulk insert errors:", body.errors);
        } else {
          console.log("employee data indexed successfully.");
        }
      } catch (error) {
        console.error("Error indexing data:", error);
      }
    });
}

//runner
(async () => {
  await createIndex();
  await indexData();
})();

//npm run dev -- to run this
