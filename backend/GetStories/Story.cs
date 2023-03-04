using System;
using Amazon.DynamoDBv2.DataModel;

namespace GetStories;

[DynamoDBTable("Stories")]
public class Story
{
    [DynamoDBHashKey]
    public string YearMonthDay { get; set; }
    
    [DynamoDBRangeKey]
    public string Title { get; set; }
    
    [DynamoDBProperty("PublishedAt")]
    public string PublishedAtString { get; set; }
    public DateTime PublishedAt => DateTime.Parse(PublishedAtString);
    
    public string[] Author { get; set; }
    public string Description { get; set; }
    public string Content { get; set; }
    public string Url { get; set; }
    public string ImageUrl { get; set; }
    public string SourceName { get; set; }
}
