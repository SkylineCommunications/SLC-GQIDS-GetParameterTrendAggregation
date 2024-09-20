namespace Getparametertrendaggregation_1
{
    using System;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net.Messages;

    /// <summary>
    /// Represents a data source.
    /// See: https://aka.dataminer.services/gqi-external-data-source for a complete example.
    /// </summary>
    [GQIMetaData(Name = "Get parameter trend aggregation")]
    public sealed class GetParameterTrendAggregation : IGQIDataSource
        , IGQIOnInit
        , IGQIInputArguments
    {
        private const string MAXIMUM = "Maximum";
        private const string AVERAGE = "Average";
        private const string MINIMUM = "Minimum";
        private const string MEAN_DEVIATION = "Mean deviation";
        private const string STANDARD_DEVIATION = "Standard deviation";

        private static readonly GQIIntArgument _dmaIDArg = new GQIIntArgument("DMA ID") { IsRequired = true };
        private static readonly GQIIntArgument _elementIDArg = new GQIIntArgument("Element ID") { IsRequired = true };
        private static readonly GQIIntArgument _parameterIDArg = new GQIIntArgument("Parameter ID") { IsRequired = true };
        private static readonly GQIStringArgument _indexIDArg = new GQIStringArgument("Index");
        private static readonly GQIDateTimeArgument _startArg = new GQIDateTimeArgument("Start") { IsRequired = true };
        private static readonly GQIDateTimeArgument _endArg = new GQIDateTimeArgument("End") { IsRequired = true };
        private static readonly GQIStringDropdownArgument _aggregationArg = new GQIStringDropdownArgument("Aggregation", new string[5] { MINIMUM, AVERAGE, MAXIMUM, MEAN_DEVIATION, STANDARD_DEVIATION }) { IsRequired = true };

        private static readonly GQIStringColumn _indexColumn = new GQIStringColumn("Index");
        private static readonly GQIDoubleColumn _aggregationColumn = new GQIDoubleColumn("Aggregation");

        private GQIDMS _dms;
        private int _dmaID;
        private int _elementID;
        private int _parameterID;
        private string _index;
        private DateTime _start;
        private DateTime _end;
        private string _aggregation;
        private GetElementProtocolResponseMessage _protocol;
        private ParameterInfo _parameter;

        private bool IsStandAlone => _parameter.ParentTable == null;

        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            _dms = args.DMS;
            return default;
        }

        public GQIArgument[] GetInputArguments()
        {
            return new GQIArgument[7]
            {
                _dmaIDArg,
                _elementIDArg,
                _parameterIDArg,
                _indexIDArg,
                _startArg,
                _endArg,
                _aggregationArg,
            };
        }

        public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
        {
            _dmaID = args.GetArgumentValue(_dmaIDArg);
            _elementID = args.GetArgumentValue(_elementIDArg);
            _parameterID = args.GetArgumentValue(_parameterIDArg);
            args.TryGetArgumentValue(_indexIDArg, out _index);
            _start = args.GetArgumentValue(_startArg);
            _end = args.GetArgumentValue(_endArg);
            _aggregation = args.GetArgumentValue(_aggregationArg);

            _protocol = _dms.SendMessage(new GetElementProtocolMessage(_dmaID, _elementID)) as GetElementProtocolResponseMessage;
            if (_protocol == null)
                throw new GenIfException($"Protocol not found for element '{_dmaID}/{_elementID}'.");

            _parameter = _protocol.FindParameter(_parameterID);
            if (_parameter == null)
                throw new GenIfException($"Parameter not found '{_parameterID}'.");

            return default;
        }

        public GQIColumn[] GetColumns()
        {
            if (IsStandAlone)
                return new GQIColumn[1] { _aggregationColumn };

            return new GQIColumn[2] { _indexColumn, _aggregationColumn };
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            if (IsStandAlone)
                return GetStandAlonePage();
            else
                return GetColumnPage();
            throw new NotSupportedException("Column table not supported.");
        }

        private GQIPage GetStandAlonePage()
        {
            var aggregation = GetStandAloneResult();
            var row = new GQIRow(new GQICell[1] { new GQICell() { Value = aggregation, DisplayValue = _parameter.GetDisplayValue(new ParameterValue(aggregation)) } });
            return new GQIPage(new GQIRow[1] { row })
            {
                HasNextPage = false,
            };
        }

        private double GetStandAloneResult()
        {
            var msg = new GetHistogramTrendDataMessage(_dmaID, _elementID, _parameterID)
            {
                StartTime = _start.ToUniversalTime(),
                EndTime = _end.ToUniversalTime(),
                DateTimeUTC = true,
                Parameters = new ParameterIndexPair[1] { new ParameterIndexPair(_parameterID, null) },
            };

            var data = _dms.SendMessage(msg) as GetHistogramTrendDataResponseMessage;

            if (data?.TrendStatistics == null || !data.TrendStatistics.TryGetValue($"{_parameterID}/", out var statistics))
                throw new GenIfException($"Could not fetch trend history for parameter '{_dmaID}/{_elementID}/{_parameterID}'.");

            if (string.Equals(_aggregation, MAXIMUM, StringComparison.OrdinalIgnoreCase))
                return statistics.Maximum;
            else if (string.Equals(_aggregation, MINIMUM, StringComparison.OrdinalIgnoreCase))
                return statistics.Minimum;
            else if (string.Equals(_aggregation, AVERAGE, StringComparison.OrdinalIgnoreCase))
                return statistics.Average;
            else if (string.Equals(_aggregation, STANDARD_DEVIATION, StringComparison.OrdinalIgnoreCase))
                return statistics.StandardDeviation;
            else if (string.Equals(_aggregation, MEAN_DEVIATION, StringComparison.OrdinalIgnoreCase))
                return statistics.MeanDeviation;
            else
                throw new GenIfException($"Could not find aggregation '{_aggregation}'.");
        }

        private GQIPage GetColumnPage()
        {
            var aggregation = GetColumnResult();
            var row = new GQIRow(new GQICell[2] 
            {
                new GQICell() { Value = _index },
                new GQICell() { Value = aggregation, DisplayValue = _parameter.GetDisplayValue(new ParameterValue(aggregation)) },
            });

            return new GQIPage(new GQIRow[1] { row })
            {
                HasNextPage = false,
            };
        }

        private double GetColumnResult()
        {
            // Logic only for one row at this moment
            var msg = new GetHistogramTrendDataMessage(_dmaID, _elementID, _parameterID)
            {
                StartTime = _start.ToUniversalTime(),
                EndTime = _end.ToUniversalTime(),
                DateTimeUTC = true,
                Parameters = new ParameterIndexPair[1] { new ParameterIndexPair(_parameterID, _index) },
            };

            var data = _dms.SendMessage(msg) as GetHistogramTrendDataResponseMessage;

            if (data?.TrendStatistics == null || !data.TrendStatistics.TryGetValue($"{_parameterID}/{_index}", out var statistics))
                throw new GenIfException($"Could not fetch trend history for parameter '{_dmaID}/{_elementID}/{_parameterID}/{_index}'.");

            if (string.Equals(_aggregation, MAXIMUM, StringComparison.OrdinalIgnoreCase))
                return statistics.Maximum;
            else if (string.Equals(_aggregation, MINIMUM, StringComparison.OrdinalIgnoreCase))
                return statistics.Minimum;
            else if (string.Equals(_aggregation, AVERAGE, StringComparison.OrdinalIgnoreCase))
                return statistics.Average;
            else if (string.Equals(_aggregation, STANDARD_DEVIATION, StringComparison.OrdinalIgnoreCase))
                return statistics.StandardDeviation;
            else if (string.Equals(_aggregation, MEAN_DEVIATION, StringComparison.OrdinalIgnoreCase))
                return statistics.MeanDeviation;
            else
                throw new GenIfException($"Could not find aggregation '{_aggregation}'.");
        }
    }
}
