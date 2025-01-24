import clientPromise from "@/lib/mongodb";
import mongoImportSchools from "@/lib/utils/mongo-helper/mongoImportSchools";
import mongoImportStates from "@/lib/utils/mongo-helper/mongoImportStates";
import mongoImportEquivalency from "@/lib/utils/mongo-helper/mongoImportEquivalency";
import mongoDailyRequestLimiter from "@/lib/utils/mongo-helper/mongoMonthlyRequestLimiter";
import { MongoClient } from "mongodb";
import { Class, School } from "@/types/mongo/mongotypes";
import getAllSubjectsInSchool from "@/lib/utils/api-helper/getAllSubjectsInSchool";
import getEquivalencyForSchool from "@/lib/utils/api-helper/getEquivalencyForSchool";

export const dynamic = "force-dynamic";

const BATCH_SIZE = 100;

export async function GET() {
  const client = await clientPromise;
  await client.connect();

  console.log("Connected to MongoDB.");

  const schoolNumber = await mongoDailyRequestLimiter(client);
  if (schoolNumber === null) {
    console.error(
      "Route limit exceeded: This route can only be accessed once per month."
    );
    return new Response(
      JSON.stringify({
        success: false,
        error: "This route can only be accessed once per month.",
      })
    );
  }

  console.log(`Starting batch process from school number ${schoolNumber}.`);

  const allSchools = await mongoImportSchools(client, states);
  const indexOfSchoolsChecked = schoolNumber + BATCH_SIZE;
  const batched = allSchools.slice(schoolNumber, indexOfSchoolsChecked);

  console.log(`Batch size: ${batched.length}.`);

  try {
    const equivalenciesForAllSchools = await gatherEquivalencies(batched);
    console.log(
      `Equivalencies gathered for ${equivalenciesForAllSchools.length} schools.`
    );
    await bulkImportEquivalencies(client, equivalenciesForAllSchools);
    console.log("Bulk import completed successfully.");
  } catch (error) {
    console.error("Error during equivalency gathering or import:", error);
  }

  const accessCollection = client.db("transfer").collection("lastAccessed");
  const newSchoolNumber =
    allSchools.length >= indexOfSchoolsChecked ? indexOfSchoolsChecked : 0;
  await accessCollection.updateOne(
    { routeName: "dailyMongoUpdate" },
    {
      $set: { schoolNumber: newSchoolNumber },
    },
    { upsert: true }
  );

  console.log(`Updated last accessed school number to ${newSchoolNumber}.`);

  return new Response(JSON.stringify({ success: true }));
}

async function gatherEquivalencies(schools: School[]) {
  const operationsPromises = schools.map((school) =>
    gatherEquivalencyForSchool(school)
  );

  const equivalenciesForAllSchools = await Promise.all(operationsPromises);
  console.log(
    `Filtered null values. Total successful equivalencies: ${
      equivalenciesForAllSchools.filter(Boolean).length
    }.`
  );
  return equivalenciesForAllSchools.filter(Boolean);
}

async function gatherEquivalencyForSchool(school: School) {
  console.log(`Processing school: ${school.name} (${school.id}).`);
  try {
    const schoolData = await getAllSubjectsInSchool(school.state, school.id);
    const term = schoolData.terms[0].id;

    const equivalents = await getEquivalencyForSchool(
      school.state,
      school.id,
      schoolData.subjects,
      term
    );

    console.log(
      `Gathered ${equivalents.length} equivalencies for ${school.name}.`
    );

    return {
      updateOne: {
        filter: { _id: school.id },
        update: {
          $set: {
            _id: school.id,
            school: school.name,
            equivalents: equivalents,
            term: term,
          },
        },
        upsert: true,
      },
    };
  } catch (error) {
    console.error(
      `Error gathering equivalency for school: ${school.name}`,
      error
    );
    return null;
  }
}

async function bulkImportEquivalencies(client: MongoClient, operations: any[]) {
  const db = client.db("transfer");
  const collection = db.collection("equivalents");
  try {
    const result = await collection.bulkWrite(operations);
    console.log(
      `Bulk import successful. Inserted: ${result.insertedCount}, Updated: ${result.modifiedCount}.`
    );
  } catch (error) {
    console.error("Bulk import failed.", error);
    throw error;
  }
}

const states = [
  "AL",
  "AK",
  "AZ",
  "AR",
  "CA",
  "CO",
  "CT",
  "DE",
  "DC",
  "FL",
  "GA",
  "GU",
  "HI",
  "ID",
  "IL",
  "IN",
  "IA",
  "KS",
  "KY",
  "LA",
  "ME",
  "MD",
  "MA",
  "MI",
  "MN",
  "MS",
  "MO",
  "MT",
  "NE",
  "NV",
  "NH",
  "NJ",
  "NM",
  "NY",
  "NC",
  "ND",
  "OH",
  "OK",
  "OR",
  "PA",
  "PR",
  "RI",
  "SC",
  "SD",
  "TN",
  "TX",
  "UT",
  "VT",
  "VI",
  "VA",
  "WA",
  "WV",
  "WI",
  "WY",
];
