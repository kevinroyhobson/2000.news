using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GetStories;

public class GetStoriesHandler
{
    private SubversionService _subversionService = new SubversionService();
    private DynamoDBContext _dynamoContext;
    
    public GetStoriesHandler() 
    {
        _dynamoContext = new DynamoDBContext(new AmazonDynamoDBClient());
    }

    public APIGatewayProxyResponse GetStories()
    {
        var stories = GetRecentStories();
        stories = stories.Select(story => _subversionService.SubvertStory(story));
        
        var body = new Dictionary<string, object>
        {
            { "PaperName", _subversionService.GetNewspaperName() },
            { "Stories", stories }, 
        };

        return new APIGatewayProxyResponse
        {
            Body = JsonConvert.SerializeObject(body),
            StatusCode = 200,
            Headers = new Dictionary<string, string> {
                { "Content-Type", "application/json" },
                { "Access-Control-Allow-Origin", "*" },
                { "Access-Control-Allow-Methods", "GET,OPTIONS" }
            }
        };
    }

    private IEnumerable<Story> GetRecentStories()
    {
        var recentStories = GetStoriesForDate(DateTime.Today);
        if (recentStories.Count < 5)
        {
            recentStories.AddRange(GetStoriesForDate(DateTime.Today.AddDays(-1)));
        }

        return recentStories.OrderBy(s => Guid.NewGuid())
                            .Take(5);
    }

    private List<Story> GetStoriesForDate(DateTime date)
    {
        return _dynamoContext.QueryAsync<Story>(date.ToString("yyyyMMdd"))
                             .GetRemainingAsync()
                             .Result;
        
    }
}

