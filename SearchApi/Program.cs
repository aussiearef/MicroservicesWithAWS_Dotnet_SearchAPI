using System.Net;
using System.Web.Http;
using Nest;
using Polly;
using Polly.CircuitBreaker;
using SearchApi.Models;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(b =>
    {
        b.AllowAnyOrigin()
            .AllowAnyMethod()
            .AllowAnyHeader();
    });
});

var app = builder.Build();
app.UseCors();


var circuitBreakerPolicy = Policy<List<Hotel>>
    .Handle<Exception>()
    .CircuitBreakerAsync(3, TimeSpan.FromSeconds(30));


app.MapGet("/search", async (string? city, int? rating) =>
{
    var result = new HttpResponseMessage(HttpStatusCode.OK);
    try
    {
        return await circuitBreakerPolicy.ExecuteAsync(async () => await SearchHotels(city, rating));
    }
    catch (BrokenCircuitException)
    {
        throw new HttpResponseException(HttpStatusCode.NotAcceptable)
        {
            Response =
            {
                Content = new StringContent("Circuit is OPEN.")
            }
        };
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
        throw;
    }
});

async Task<List<Hotel>> SearchHotels(string? city, int? rating)
{
    var host = Environment.GetEnvironmentVariable("host");
    var userName = Environment.GetEnvironmentVariable("userName");
    var password = Environment.GetEnvironmentVariable("password");
    var indexName = Environment.GetEnvironmentVariable("indexName");

    var conSett = new ConnectionSettings(new Uri(host));
    conSett.BasicAuthentication(userName, password);
    conSett.DefaultIndex(indexName);
    conSett.DefaultMappingFor<Hotel>(m => m.IdProperty(p => p.Id));
    var client = new ElasticClient(conSett);

    rating ??= 1;

    // Match 
    // Prefix 
    // Range
    // Fuzzy Match

    ISearchResponse<Hotel> result;

    if (city is null)
        result = await client.SearchAsync<Hotel>(s => s.Query(q =>
            q.MatchAll() &&
            q.Range(r => r.Field(f => f.Rating).GreaterThanOrEquals(rating))
        ));
    else
        result = await client.SearchAsync<Hotel>(s =>
            s.Query(q =>
                q.Prefix(p => p.Field(f => f.CityName).Value(city).CaseInsensitive())
                &&
                q.Range(r => r.Field(f => f.Rating).GreaterThanOrEquals(rating))
            )
        );

    return result.Hits.Select(x => x.Source).ToList();
}

app.Run();