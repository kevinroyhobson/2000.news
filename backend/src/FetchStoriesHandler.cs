using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using NewsAPI;
using NewsAPI.Constants;
using NewsAPI.Models;

namespace News2000
{
    public class FetchStoriesHandler
    {
        private NewsApiClient _newsApiClient;
        private AmazonDynamoDBClient _dynamoClient;

        public FetchStoriesHandler()
        {
            var newsApiKey = Environment.GetEnvironmentVariable("NEWS_API_KEY");
            _newsApiClient = new NewsApiClient(newsApiKey);

            _dynamoClient = new AmazonDynamoDBClient();
        }

        [LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
        public APIGatewayProxyResponse FetchStories()
        {
            var articlesResponse = _newsApiClient.GetTopHeadlines(new TopHeadlinesRequest()
            {
                Country = Countries.US,
                Language = Languages.EN
            });

            if (articlesResponse.Status == Statuses.Ok)
            {
                foreach (var article in articlesResponse.Articles)
                {
                    SaveStory(article);
                }
            }

            string responseMessage = articlesResponse.Status == Statuses.Ok
                       ? $"Processed {articlesResponse.Articles.Count} stories."
                       : $"Error: {articlesResponse.Status}. Processed zero stories.";
            var body = new Dictionary<string, string>
            {
                { "message", responseMessage }
            };
            return new APIGatewayProxyResponse
            {
                Body = JsonConvert.SerializeObject(body),
                StatusCode = 200,
                Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
            };
        }

        private void SaveStory(Article article)
        {
            try
            {
                var putRequest = new PutItemRequest("Stories", new Dictionary<string, AttributeValue>()
                {
                    {"YearMonthDay", GetAttributeValue(article.PublishedAt?.ToString("yyyyMMdd"))},
                    {"PublishedAt", GetAttributeValue(article.PublishedAt?.ToString("s", CultureInfo.InvariantCulture))},
                    {"Title", GetAttributeValue(GetTitleWithoutSource(article))},
                    {"Description", GetAttributeValue(article.Description)},
                    {"Author", GetAttributeValue(article.Author)},
                    {"Content", GetAttributeValue(article.Content)},
                    {"Url", GetAttributeValue(article.Url)},
                    {"ImageUrl", GetAttributeValue(article.UrlToImage)},
                    {"SourceName", GetAttributeValue(article.Source.Name)}
                });

                var putResponse = _dynamoClient.PutItemAsync(putRequest).Result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private AttributeValue GetAttributeValue(string value)
        {
            return new AttributeValue(value ?? string.Empty);
        }

        private string GetTitleWithoutSource(Article article)
        {
            Regex sourceRegex = new Regex(" [-–|] .*$");
            return sourceRegex.Replace(article.Title, string.Empty);
        }
    }
}
