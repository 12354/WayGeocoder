using Konsole;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace WayGeocoder
{

    public class NominatimXML
    {
        public int place_id { get; set; }
        public string licence { get; set; }
        public string osm_type { get; set; }
        public int osm_id { get; set; }
        public string lat { get; set; }
        public string lon { get; set; }
        public string display_name { get; set; }
        public Address address { get; set; }
        public string[] boundingbox { get; set; }
    }

    public class Address
    {
        public string railway { get; set; }
        public string road { get; set; }
        public string neighbourhood { get; set; }
        public string suburb { get; set; }
        public string city_district { get; set; }
        public string city { get; set; }
        public string village { get; set; }
        public string state { get; set; }
        public string postcode { get; set; }
        public string country { get; set; }
        public string country_code { get; set; }
    }


    internal class Program
    {
        static string template = "http://{0}:8080/reverse?format=json&osm_type=W&osm_id={1}";
        static string uri = "";
        static void Main(string[] args)
        {
             Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            if (args.Length == 0)
            {
                Console.WriteLine("Kein Input angegeben");
                Environment.Exit(1);
            }
            if (args.Length == 1)
            {
                Console.WriteLine("Kein Output angegeben");
                Environment.Exit(1);
            }
            var ip = File.ReadAllText("server.txt");
            uri = string.Format(template, ip, "{0}");
            var ways = File.ReadAllLines(args[0]);
            if (ways.Length <= 1)
            {
                Console.WriteLine("no ways");
                Environment.Exit(1);
            }
            ways = ways.Skip(1).ToArray();
            Console.WriteLine($"Found {ways.Length} ways");

            //var bar = new ProgressBar(ways.Length);
            //bar.Refresh(0, ways[0]);
            var httpClient = new HttpClient()
            {
                Timeout = TimeSpan.FromMinutes(2)
            };

            var result = new ConcurrentBag<string>();
            var errors = new ConcurrentBag<string>();

            int count = 0;
            int data_missing = 0;
            int unable_to_geocode = 0;
            int exception = 0;
            Parallel.ForEach(ways,new ParallelOptions()
            {
                MaxDegreeOfParallelism= 6,
            },
                way =>
             {
                 int current = Interlocked.Increment(ref count);

                 try
                 {
                     var response = httpClient.GetStringAsync(string.Format(uri, way)).Result;
                     if (response.Contains("Unable to geocode"))
                     {
                         errors.Add(way);
                         //   bar.Next("Error: " + way);
                         Interlocked.Increment(ref unable_to_geocode);


                         if (current % 1000 == 0)
                         {
                             Console.WriteLine($"Count: {current}/{ways.Length} Errors: {errors.Count}/G{unable_to_geocode}/D{data_missing}/E{exception}");
                         }
                         return;
                     }
                     var json = JsonSerializer.Deserialize<NominatimXML>(response);
                     if ((json?.address?.city ?? json?.address?.village) == null || json?.address?.road == null)
                     {
                         Interlocked.Increment(ref data_missing);

                         if (current % 1000 == 0)
                         {
                             Console.WriteLine($"Count: {current}/{ways.Length} Errors: {errors.Count}/G{unable_to_geocode}/D{data_missing}/E{exception}");
                         }
                         return;
                     }
                     var csvLine = $"{json.address.road};;;{json.address.city ?? json.address.village};{json.address.city_district ?? json.address.city ?? json.address.suburb};{json.address.postcode};;;;;;{json.lon.ToString().Replace(".", ",")};{json.lat.ToString().Replace(".", ",")};;;";
                     result.Add(csvLine);
                     if (current % 1000 == 0)
                     {
                         Console.WriteLine($"Count: {current}/{ways.Length} Errors: {errors.Count}/G{unable_to_geocode}/D{data_missing}/E{exception}");
                     }

                 }
                 catch(Exception ex)
                 {
                     //Console.WriteLine(ex.ToString());
                     Interlocked.Increment(ref exception);

                     errors.Add(way);
                     if (current % 1000 == 0)
                     {
                         Console.WriteLine($"Count: {current}/{ways.Length} Errors: {errors.Count}/G{unable_to_geocode}/D{data_missing}/E{exception}");
                     }
                     //bar.Next("Error: " + way);
                 }
             });
            Console.WriteLine("Number of errors: " + errors.Count);
            File.WriteAllLines("errors.txt", errors);
            Console.WriteLine("Wrote errors to errors.txt");

            var resultSb = new StringBuilder();
            resultSb.AppendLine("STRASSE;SYNONYM;POS;ORT;ORTSTEIL;PLZ;AAO_GEBIET;ABSCHNITT;INFO_PFAD;ANFAHRT;HINWEIS;KOORDX;KOORDY;HAUSNUMMER_MIN;HAUSNUMMER_MAX;");
            var resultHashSet = new HashSet<string>();
            foreach (var way in result)
            {
                var firstPart = Regex.Match(way, "(.*?);;;(.*?);(.*?);(.*?);;;;;;");
                if (firstPart.Success)
                {
                    if (resultHashSet.Contains(firstPart.Value))
                    {
                        continue;
                    }
                    resultHashSet.Add(firstPart.Value);
                }

                resultSb.AppendLine(way);
            }
            File.WriteAllText(args[1], resultSb.ToString(),Encoding.GetEncoding("windows-1252"));
        }
    }
}