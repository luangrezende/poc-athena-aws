using Amazon.Athena;
using Amazon.Athena.Model;
using POC.Athena.Aws.Models;
using System.Net;

namespace POC.Athena.Aws
{
    public class AthenaTest
    {
        static AmazonAthenaClient _client = null!;
        const int PersonId = 0, FirstName = 1, City = 2;

        public AthenaTest()
        {
            _client = new AmazonAthenaClient(
                    Environment.GetEnvironmentVariable("AwsAccessKeyId"),
                    Environment.GetEnvironmentVariable("AwsSecretAccessKey"),
                    Amazon.RegionEndpoint.USEast1);
        }

        public async Task<PersonModel> GetPersonById(string id)
        {
            try
            {
                var queryRequest = BuildQueryRequest($"SELECT * FROM person WHERE id = '{id}'");

                var result = await _client.StartQueryExecutionAsync(queryRequest);
                var results = await GetQueryResult(result.QueryExecutionId, null!);
                var first = results.ResultSet.Rows.Skip(1).FirstOrDefault();

                return new PersonModel
                {
                    Id = first?.Data[PersonId]?.VarCharValue,
                    FirstName = first?.Data[FirstName]?.VarCharValue,
                    City = first?.Data[City]?.VarCharValue,
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                throw;
            }
        }

        //public async Task Delete(string id)
        //{
        //    try
        //    {
        //        var queryRequest = BuildQueryRequest($"DELETE FROM person WHERE id = '{id}'");
        //        var result = await _client.StartQueryExecutionAsync(queryRequest);
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"Error: {ex.Message}");
        //        throw;
        //    }
        //}


        public async Task Create(Guid Id, string firstName, string city)
        {
            try
            {
                var queryRequest = BuildQueryRequest($"INSERT INTO person VALUES('{Id}', '{firstName}', '{city}')");

                var result = await _client.StartQueryExecutionAsync(queryRequest);

                if (result.HttpStatusCode == HttpStatusCode.OK)
                    Console.WriteLine($"Success.");
                else
                    Console.WriteLine($"Error while creating a new person.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                throw;
            }
        }

        public async Task<List<PersonModel>> GetAll()
        {
            try
            {
                var queryRequest = BuildQueryRequest($"SELECT * FROM person");

                var personList = new List<PersonModel>();
                var result = await _client.StartQueryExecutionAsync(queryRequest);
                var results = await GetQueryResult(result.QueryExecutionId, null!);

                foreach (var person in results.ResultSet.Rows.Skip(1))
                {
                    personList.Add(new PersonModel
                    {
                        Id = person.Data[PersonId].VarCharValue,
                        FirstName = person.Data[FirstName].VarCharValue,
                        City = person.Data[City].VarCharValue,
                    });
                }

                return personList;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                throw;
            }
        }

        private static async Task<GetQueryResultsResponse> GetQueryResult(string queryExecutionId, string token)
        {
            GetQueryResultsResponse results = null!;
            bool succeeded = false;
            int retries = 0;
            const int max_retries = 10;

            while (!succeeded)
            {
                try
                {
                    results = await _client.GetQueryResultsAsync(new GetQueryResultsRequest()
                    {
                        QueryExecutionId = queryExecutionId,
                        NextToken = token
                    });
                    succeeded = true;
                }
                catch (InvalidRequestException ex)
                {
                    if (ex.Message.EndsWith("QUEUED") || ex.Message.EndsWith("RUNNING"))
                    {
                        Thread.Sleep(1000 * 30);
                        retries++;
                        if (retries >= max_retries) throw ex;
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }

            return results;
        }
        
        private StartQueryExecutionRequest BuildQueryRequest(string query)
        {
            return new StartQueryExecutionRequest
            {
                QueryString = query,
                QueryExecutionContext = new QueryExecutionContext()
                {
                    Database = Environment.GetEnvironmentVariable("AwsAthenaDatabase")
                },
                ResultConfiguration = new ResultConfiguration
                {
                    OutputLocation = Environment.GetEnvironmentVariable("AwsS3BucketLocation")
                }
            };
        }

        //public async Task GetAll()
        //{
        //    try
        //    {
        //        var queryRequest = new StartQueryExecutionRequest
        //        {
        //            QueryString = $"SELECT * FROM person",
        //            QueryExecutionContext = new QueryExecutionContext()
        //            {
        //                Database = "db_athena_vuc_dev"
        //            },
        //            ResultConfiguration = new ResultConfiguration
        //            {
        //                OutputLocation = "s3://bucket-vuc-athena-dev/"
        //            }
        //        };

        //        var result = await _client.StartQueryExecutionAsync(queryRequest);

        //        Row index = null!;
        //        GetQueryResultsResponse results = null!;
        //        bool firstTime = true;
        //        string token = null!;

        //        string queryExecutionId = result.QueryExecutionId;
        //        while (firstTime || token != null)
        //        {
        //            token = (firstTime ? null! : token);
        //            results = await GetQueryResult(queryExecutionId, token);

        //            int skipCount = 0;
        //            if (firstTime)
        //            {
        //                skipCount = 1;
        //                index = results.ResultSet.Rows[0];
        //            }

        //            foreach (var person in results.ResultSet.Rows.Skip(skipCount))
        //            {
        //                Console.WriteLine($"Person: {person.Data[PersonId].VarCharValue} / {person.Data[FirstName].VarCharValue} / {person.Data[City].VarCharValue}");
        //            }

        //            token = results.NextToken;
        //            firstTime = false;
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"Error: {ex.Message}");
        //    }
        //}
    }
}