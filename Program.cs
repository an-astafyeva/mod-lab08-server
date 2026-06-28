using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SmoModeling
{
    public class Request
    {
        public int ClientId { get; }
        public DateTime GenerationTime { get; }
        public DateTime? StartTime  { get; set; }
        public DateTime? EndTime    { get; set; }

        public Request(int clientId, DateTime generationTime)
        {
            ClientId       = clientId;
            GenerationTime = generationTime;
        }
    }

    public class RequestEventArgs : EventArgs
    {
        public Request Request { get; }
        public RequestEventArgs(Request request) => Request = request;
    }

    public class ServiceChannel
    {
        public int  Id     { get; }
        public bool IsBusy { get; internal set; }

        private readonly Random _rng;
        private readonly double _serviceRate;

        public ServiceChannel(int id, double serviceRate)
        {
            Id           = id;
            _serviceRate = serviceRate;
            IsBusy       = false;
            _rng = new Random(Guid.NewGuid().GetHashCode());
        }

        public async Task ProcessRequest(Request request)
        {
            request.StartTime = DateTime.Now;
            double serviceTime = -Math.Log(1.0 - _rng.NextDouble()) / _serviceRate;
            await Task.Delay(TimeSpan.FromSeconds(serviceTime));
            request.EndTime = DateTime.Now;
        }
    }

    public class Server
    {
        private readonly List<ServiceChannel> _channels;
        private readonly object _lock = new object();

        private int    _totalRequests     = 0;
        private int    _processedRequests = 0;
        private int    _rejectedRequests  = 0;

        private double _busyChannelSeconds = 0.0;
        private DateTime _lastSnapshotTime;
        private readonly DateTime _startTime;

        public Server(int channelCount, double serviceRate)
        {
            _channels = new List<ServiceChannel>();
            for (int i = 0; i < channelCount; i++)
                _channels.Add(new ServiceChannel(i + 1, serviceRate));

            _startTime        = DateTime.Now;
            _lastSnapshotTime = _startTime;
        }

        public void SubscribeClient(Client client)
        {
            client.RequestGenerated += OnRequestReceived;
        }

        private async void OnRequestReceived(object sender, RequestEventArgs e)
        {
            try
            {
                ServiceChannel? freeChannel = null;

                lock (_lock)
                {
                    TakeSnapshot();
                    _totalRequests++;

                    freeChannel = _channels.FirstOrDefault(c => !c.IsBusy);
                    if (freeChannel != null)
                    {
                        freeChannel.IsBusy = true;
                        _processedRequests++;
                    }
                    else
                    {
                        _rejectedRequests++;
                    }
                }

                if (freeChannel != null)
                {
                    await freeChannel.ProcessRequest(e.Request);

                    lock (_lock)
                    {
                        TakeSnapshot();
                        freeChannel.IsBusy = false;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[Server] Ошибка: {ex.Message}");
            }
        }

        private void TakeSnapshot()
        {
            var now   = DateTime.Now;
            double dt = (now - _lastSnapshotTime).TotalSeconds;
            int busy  = _channels.Count(c => c.IsBusy);
            _busyChannelSeconds += busy * dt;
            _lastSnapshotTime   = now;
        }

        public Statistics GetStatistics()
        {
            lock (_lock)
            {
                TakeSnapshot();
                double uptime = (DateTime.Now - _startTime).TotalSeconds;
                double avgBusy = uptime > 0 ? _busyChannelSeconds / uptime : 0;

                return new Statistics
                {
                    TotalRequests     = _totalRequests,
                    ProcessedRequests = _processedRequests,
                    RejectedRequests  = _rejectedRequests,
                    AvgBusyChannels   = avgBusy,
                    Uptime            = uptime,
                    ChannelCount      = _channels.Count
                };
            }
        }

        public void Reset()
        {
            lock (_lock)
            {
                _totalRequests      = 0;
                _processedRequests  = 0;
                _rejectedRequests   = 0;
                _busyChannelSeconds = 0;
                _lastSnapshotTime   = DateTime.Now;
            }
        }
    }

    public class Statistics
    {
        public int    TotalRequests     { get; set; }
        public int    ProcessedRequests { get; set; }
        public int    RejectedRequests  { get; set; }
        public double AvgBusyChannels   { get; set; }
        public double Uptime            { get; set; }
        public int    ChannelCount      { get; set; }

        public double ProbabilityRejection =>
            TotalRequests > 0 ? (double)RejectedRequests / TotalRequests : 0;

        public double RelativeThroughput =>
            TotalRequests > 0 ? (double)ProcessedRequests / TotalRequests : 0;

        public double AbsoluteThroughput =>
            Uptime > 0 ? ProcessedRequests / Uptime : 0;

        public double ProbabilityIdle =>
            ChannelCount > 0 ? Math.Max(0, 1.0 - AvgBusyChannels / ChannelCount) : 0;
    }

    public class Client
    {
        private static int _nextId = 1;
        public int Id { get; }

        private readonly double _requestRate;
        private readonly Random _rng = new Random(Guid.NewGuid().GetHashCode());

        public event EventHandler<RequestEventArgs>? RequestGenerated;

        public Client(double requestRate)
        {
            Id           = _nextId++;
            _requestRate = requestRate;
        }

        public async Task StartGeneratingRequests(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                double interval = -Math.Log(1.0 - _rng.NextDouble()) / _requestRate;
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(interval), ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                var request = new Request(Id, DateTime.Now);
                RequestGenerated?.Invoke(this, new RequestEventArgs(request));
            }
        }
    }

    public static class SmoTheory
    {
        public static TheoreticalResults Calculate(double lambda, double mu, int n)
        {
            double rho = lambda / mu;

            double sum = 0;
            for (int k = 0; k <= n; k++)
                sum += Math.Pow(rho, k) / Factorial(k);
            double p0 = 1.0 / sum;

            double pRejection = (Math.Pow(rho, n) / Factorial(n)) * p0;

            double Q    = 1.0 - pRejection;
            double A    = lambda * Q;
            double kAvg = rho * Q;

            return new TheoreticalResults
            {
                ProbabilityIdle      = p0,
                ProbabilityRejection = pRejection,
                RelativeThroughput   = Q,
                AbsoluteThroughput   = A,
                AvgBusyChannels      = kAvg
            };
        }

        private static double Factorial(int n)
        {
            double r = 1;
            for (int i = 2; i <= n; i++) r *= i;
            return r;
        }
    }

    public class TheoreticalResults
    {
        public double ProbabilityIdle      { get; set; }
        public double ProbabilityRejection { get; set; }
        public double RelativeThroughput   { get; set; }
        public double AbsoluteThroughput   { get; set; }
        public double AvgBusyChannels      { get; set; }
    }

    public class DataPoint
    {
        public double Lambda         { get; set; }
        public double ExpIdle        { get; set; }
        public double ExpRejection   { get; set; }
        public double ExpRelative    { get; set; }
        public double ExpAbsolute    { get; set; }
        public double ExpAvgBusy     { get; set; }
        public double TheoIdle       { get; set; }
        public double TheoRejection  { get; set; }
        public double TheoRelative   { get; set; }
        public double TheoAbsolute   { get; set; }
        public double TheoAvgBusy    { get; set; }
    }

    class Program
    {
        private const int    ChannelCount      = 5;
        private const double Mu                = 2.0;
        private const int    SimulationSeconds = 30;
        private const int    ClientCount       = 5;

        static async Task Main(string[] args)
        {
            Console.WriteLine("МОДЕЛИРОВАНИЕ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ (M/M/n/n)");
            Console.WriteLine($"  Каналов n = {ChannelCount},  μ = {Mu} зап/с,  время опыта = {SimulationSeconds} с");
            Console.WriteLine();

            var lambdaValues = Enumerable.Range(1, 11)
                                         .Select(i => Math.Round(i * 0.5, 1))
                                         .ToList();

            var points = new List<DataPoint>();

            foreach (var lambda in lambdaValues)
            {
                Console.Write($"  λ = {lambda:F1} ... ");

                var server  = new Server(ChannelCount, Mu);
                var clients = new List<Client>();
                var cts     = new CancellationTokenSource();

                double clientRate = lambda / ClientCount;
                for (int i = 0; i < ClientCount; i++)
                {
                    var c = new Client(clientRate);
                    server.SubscribeClient(c);
                    clients.Add(c);
                }

                var tasks = clients
                    .Select(c => c.StartGeneratingRequests(cts.Token))
                    .ToArray();

                await Task.Delay(TimeSpan.FromSeconds(SimulationSeconds));
                cts.Cancel();

                try { await Task.WhenAll(tasks); }
                catch (OperationCanceledException) { }

                var stats = server.GetStatistics();
                var theor = SmoTheory.Calculate(lambda, Mu, ChannelCount);

                var pt = new DataPoint
                {
                    Lambda        = lambda,
                    ExpIdle       = stats.ProbabilityIdle,
                    ExpRejection  = stats.ProbabilityRejection,
                    ExpRelative   = stats.RelativeThroughput,
                    ExpAbsolute   = stats.AbsoluteThroughput,
                    ExpAvgBusy    = stats.AvgBusyChannels,
                    TheoIdle      = theor.ProbabilityIdle,
                    TheoRejection = theor.ProbabilityRejection,
                    TheoRelative  = theor.RelativeThroughput,
                    TheoAbsolute  = theor.AbsoluteThroughput,
                    TheoAvgBusy   = theor.AvgBusyChannels,
                };
                points.Add(pt);

                Console.WriteLine($"поступило={stats.TotalRequests,5}  " +
                                  $"обслужено={stats.ProcessedRequests,5}  " +
                                  $"отказов={stats.RejectedRequests,5}  " +
                                  $"Pотк(эксп)={stats.ProbabilityRejection:F4}  " +
                                  $"Pотк(теор)={theor.ProbabilityRejection:F4}");
            }

            Directory.CreateDirectory("result");
            SaveResultsTxt(points);
            SaveCsv(points);

            Console.WriteLine();
            Console.WriteLine("Готово! Файлы:");
            Console.WriteLine("  result/results.txt");
            Console.WriteLine("  result/data.csv");
            Console.WriteLine();
            Console.WriteLine("Для построения графиков запустите:");
            Console.WriteLine("  python3 result/plot.py");
        }

        static void SaveResultsTxt(List<DataPoint> pts)
        {
            using var w = new StreamWriter("result/results.txt", false, System.Text.Encoding.UTF8);
            w.WriteLine("РЕЗУЛЬТАТЫ МОДЕЛИРОВАНИЯ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ (M/M/n/n)");
            w.WriteLine("=================================================================");
            w.WriteLine($"n = {ChannelCount} каналов,  μ = {Mu} зап/с,  время опыта = {SimulationSeconds} с");
            w.WriteLine();

            string hdr = $"{"λ",5} | {"P0 эксп",8} | {"P0 теор",8} | {"Pотк эксп",10} | {"Pотк теор",10}" +
                         $" | {"Q эксп",8} | {"Q теор",8} | {"A эксп",8} | {"A теор",8}" +
                         $" | {"k эксп",8} | {"k теор",8}";
            w.WriteLine(hdr);
            w.WriteLine(new string('-', hdr.Length));

            foreach (var p in pts)
            {
                w.WriteLine($"{p.Lambda,5:F1} | {p.ExpIdle,8:F4} | {p.TheoIdle,8:F4} | " +
                            $"{p.ExpRejection,10:F4} | {p.TheoRejection,10:F4} | " +
                            $"{p.ExpRelative,8:F4} | {p.TheoRelative,8:F4} | " +
                            $"{p.ExpAbsolute,8:F4} | {p.TheoAbsolute,8:F4} | " +
                            $"{p.ExpAvgBusy,8:F4} | {p.TheoAvgBusy,8:F4}");
            }

            w.WriteLine();
            w.WriteLine("ВЫВОДЫ:");
            w.WriteLine("1. С ростом λ вероятность отказа (Pотк) монотонно возрастает.");
            w.WriteLine("2. Относительная пропускная способность Q убывает с ростом λ.");
            w.WriteLine("3. Абсолютная пропускная способность A сначала растёт, затем насыщается.");
            w.WriteLine("4. При λ >> μ·n система насыщена, большинство запросов получают отказ.");
            w.WriteLine("5. Экспериментальные данные хорошо согласуются с теоретическими.");
        }

        static void SaveCsv(List<DataPoint> pts)
        {
            // Сохраняем данные в CSV для построения графиков через Python
            using var w = new StreamWriter("result/data.csv", false, System.Text.Encoding.UTF8);
            w.WriteLine("lambda,exp_idle,theo_idle,exp_rejection,theo_rejection," +
                        "exp_relative,theo_relative,exp_absolute,theo_absolute," +
                        "exp_avg_busy,theo_avg_busy");

            foreach (var p in pts)
            {
                w.WriteLine(string.Format(
                    System.Globalization.CultureInfo.InvariantCulture,
                    "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10}",
                    p.Lambda,
                    p.ExpIdle, p.TheoIdle,
                    p.ExpRejection, p.TheoRejection,
                    p.ExpRelative, p.TheoRelative,
                    p.ExpAbsolute, p.TheoAbsolute,
                    p.ExpAvgBusy, p.TheoAvgBusy));
            }

            // Записываем рядом Python-скрипт для графиков
            using var py = new StreamWriter("result/plot.py", false, System.Text.Encoding.UTF8);
            py.WriteLine(@"import csv
import matplotlib.pyplot as plt
import os

data = []
with open(os.path.join(os.path.dirname(__file__), 'data.csv'), newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        data.append({k: float(v) for k, v in row.items()})

xs = [d['lambda'] for d in data]

charts = [
    ('exp_idle',       'theo_idle',       'Вероятность простоя системы P₀',          'P₀',    'p-1.png'),
    ('exp_rejection',  'theo_rejection',  'Вероятность отказа Pотк',                  'Pотк',  'p-2.png'),
    ('exp_relative',   'theo_relative',   'Относительная пропускная способность Q',   'Q',     'p-3.png'),
    ('exp_absolute',   'theo_absolute',   'Абсолютная пропускная способность A',      'A',     'p-4.png'),
    ('exp_avg_busy',   'theo_avg_busy',   'Среднее число занятых каналов k',          'k',     'p-5.png'),
]

out_dir = os.path.dirname(__file__)

for exp_key, theo_key, title, ylabel, filename in charts:
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(xs, [d[exp_key]  for d in data], 'o-', color='steelblue', label='Экспериментальные')
    ax.plot(xs, [d[theo_key] for d in data], 's-', color='crimson',   label='Теоретические')
    ax.set_title(title)
    ax.set_xlabel('Интенсивность входного потока λ (зап/с)')
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=100)
    plt.close(fig)
    print(f'Сохранён {filename}')

print('Все графики готовы.')
");
        }
    }
}
