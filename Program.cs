using POC.Athena.Aws;

var athenaTest = new AthenaTest();

//GetById
var person = await athenaTest.GetPersonById("cbb5e731-3b0d-4207-9b78-98f69d2c04f2");

//GetAll
var personList = await athenaTest.GetAll();

//Create
await athenaTest.Create(Guid.NewGuid(), "Luan", "Santos");

//Delete
//await athenaTest.Delete("cbb5e731-3b0d-4207-9b78-98f69d2c04f2");
