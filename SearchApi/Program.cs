using Nest;
using SearchApi.Models;

var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();


app.MapGet("/search", async (string? city, int? rating) =>
{
    var host = "https://hotels.es.australia-southeast1.gcp.elastic-cloud.com";
    var userName = "elastic";
    var password = "PxnPsTFYqveJ7TzYm4rHjdCd";
    var indexName = "event";

    var conSett = new ConnectionSettings(new Uri(host));
    conSett.BasicAuthentication(userName, password);
    conSett.DefaultIndex(indexName);
    conSett.DefaultMappingFor<Hotel>(m => m.IdProperty(p => p.Id));
    var client = new ElasticClient(conSett);

    if (rating is null) rating = 1;

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
});

app.Run();