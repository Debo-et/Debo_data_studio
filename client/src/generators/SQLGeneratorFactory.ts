// src/generators/SQLGeneratorFactory.ts

import { BaseSQLGenerator, SQLGenerationOptions, SelectSQLGenerator } from './BaseSQLGenerator';
import { MapSQLGenerator } from './MapSQLGenerator';
import { JoinSQLGenerator } from './JoinSQLGenerator';
import { FilterSQLGenerator } from './FilterSQLGenerator';
import { AggregateSQLGenerator } from './AggregateSQLGenerator';
import { SortSQLGenerator } from './SortSQLGenerator';
import { LookupSQLGenerator } from './LookupSQLGenerator';
import { InputSQLGenerator } from './InputSQLGenerator';
import { OutputSQLGenerator } from './OutputSQLGenerator';
import { ReplaceSQLGenerator } from './ReplaceSQLGenerator';
import { ConvertTypeSQLGenerator } from './ConvertTypeSQLGenerator';
import { ExtractDelimitedSQLGenerator } from './ExtractDelimitedSQLGenerator';
import { ExtractRegexSQLGenerator } from './ExtractRegexSQLGenerator';
import { ExtractJSONSQLGenerator } from './ExtractJSONSQLGenerator';
import { ExtractXMLSQLGenerator } from './ExtractXMLSQLGenerator';
import { ParseRecordSetSQLGenerator } from './ParseRecordSetSQLGenerator';
import { SplitRowSQLGenerator } from './SplitRowSQLGenerator';
import { PivotSQLGenerator } from './PivotSQLGenerator';
import { UnpivotSQLGenerator } from './UnpivotSQLGenerator';
import { UniqueRowSQLGenerator } from './UniqueRowSQLGenerator';
import { SampleRowSQLGenerator } from './SampleRowSQLGenerator';
import { SchemaComplianceSQLGenerator } from './SchemaComplianceSQLGenerator';
import { AddCRCSQLGenerator } from './AddCRCSQLGenerator';
import { StandardizeRowSQLGenerator } from './StandardizeRowSQLGenerator';
import { DataMaskingSQLGenerator } from './DataMaskingSQLGenerator';
import { AssertSQLGenerator } from './AssertSQLGenerator';
import { MatchGroupSQLGenerator } from './MatchGroupSQLGenerator';
import { RowGeneratorSQLGenerator } from './RowGeneratorSQLGenerator';
import { NormalizeNumberSQLGenerator } from './NormalizeNumberSQLGenerator';
import { FileLookupSQLGenerator } from './FileLookupSQLGenerator';
import { CacheInSQLGenerator, CacheOutSQLGenerator } from './CacheSQLGenerator';
import { RecordMatchingSQLGenerator } from './RecordMatchingSQLGenerator';
import { DenormalizeSQLGenerator } from './DenormalizeSQLGenerator';
import { NormalizeSQLGenerator } from './NormalizeSQLGenerator';
import {
  FlowToIterateSQLGenerator,
  IterateToFlowSQLGenerator,
  ReplicateSQLGenerator,
  UniteSQLGenerator,
  FlowMergeSQLGenerator,
  FlowMeterSQLGenerator,
  FlowMeterCatcherSQLGenerator
} from './FlowControlSQLGenerator';
import { NodeType } from '../types/unified-pipeline.types';

export class SQLGeneratorFactory {
  static createGenerator(nodeType: NodeType, options: Partial<SQLGenerationOptions> = {}): BaseSQLGenerator {
    switch (nodeType) {
      case NodeType.MAP:
        return new MapSQLGenerator(options);
      case NodeType.JOIN:
        return new JoinSQLGenerator(options);
      case NodeType.FILTER_ROW:
        return new FilterSQLGenerator(options);
      case NodeType.AGGREGATE_ROW:
        return new AggregateSQLGenerator(options);
      case NodeType.SORT_ROW:
        return new SortSQLGenerator(options);
      case NodeType.LOOKUP:
        return new LookupSQLGenerator(options);
      case NodeType.INPUT:
        return new InputSQLGenerator(options);
      case NodeType.OUTPUT:
        return new OutputSQLGenerator(options);
      case NodeType.REPLACE:
      case NodeType.REPLACE_LIST:
        return new ReplaceSQLGenerator(options);
      case NodeType.CONVERT_TYPE:
        return new ConvertTypeSQLGenerator(options);
      case NodeType.EXTRACT_DELIMITED_FIELDS:
        return new ExtractDelimitedSQLGenerator(options);
      case NodeType.EXTRACT_REGEX_FIELDS:
        return new ExtractRegexSQLGenerator(options);
      case NodeType.EXTRACT_JSON_FIELDS:
        return new ExtractJSONSQLGenerator(options);
      case NodeType.EXTRACT_XML_FIELD:
        return new ExtractXMLSQLGenerator(options);
      case NodeType.PARSE_RECORD_SET:
        return new ParseRecordSetSQLGenerator(options);
      case NodeType.SPLIT_ROW:
        return new SplitRowSQLGenerator(options);
      case NodeType.PIVOT_TO_COLUMNS_DELIMITED:
        return new PivotSQLGenerator(options);
      case NodeType.UNPIVOT_ROW:
        return new UnpivotSQLGenerator(options);
      case NodeType.UNIQ_ROW:
        return new UniqueRowSQLGenerator(options);
      case NodeType.SAMPLE_ROW:
        return new SampleRowSQLGenerator(options);
      case NodeType.SCHEMA_COMPLIANCE_CHECK:
        return new SchemaComplianceSQLGenerator(options);
      case NodeType.ADD_CRC_ROW:
      case NodeType.ADD_CRC:
        return new AddCRCSQLGenerator(options);
      case NodeType.STANDARDIZE_ROW:
        return new StandardizeRowSQLGenerator(options);
      case NodeType.DATA_MASKING:
        return new DataMaskingSQLGenerator(options);
      case NodeType.ASSERT:
        return new AssertSQLGenerator(options);
      case NodeType.MATCH_GROUP:
        return new MatchGroupSQLGenerator(options);
      case NodeType.ROW_GENERATOR:
        return new RowGeneratorSQLGenerator(options);
      case NodeType.NORMALIZE_NUMBER:
        return new NormalizeNumberSQLGenerator(options);
      case NodeType.FILE_LOOKUP:
        return new FileLookupSQLGenerator(options);
      case NodeType.CACHE_IN:
        return new CacheInSQLGenerator(options);
      case NodeType.CACHE_OUT:
        return new CacheOutSQLGenerator(options);
      case NodeType.RECORD_MATCHING:
        return new RecordMatchingSQLGenerator(options);
      case NodeType.DENORMALIZE:
      case NodeType.DENORMALIZE_SORTED_ROW:
        return new DenormalizeSQLGenerator(options);
      case NodeType.NORMALIZE:
        return new NormalizeSQLGenerator(options);
      case NodeType.FLOW_TO_ITERATE:
        return new FlowToIterateSQLGenerator(options);
      case NodeType.ITERATE_TO_FLOW:
        return new IterateToFlowSQLGenerator(options);
      case NodeType.REPLICATE:
        return new ReplicateSQLGenerator(options);
      case NodeType.UNITE:
        return new UniteSQLGenerator(options);
      case NodeType.FLOW_MERGE:
        return new FlowMergeSQLGenerator(options);
      case NodeType.FLOW_METER:
        return new FlowMeterSQLGenerator(options);
      case NodeType.FLOW_METER_CATCHER:
        return new FlowMeterCatcherSQLGenerator(options);
      default:
        console.warn(`No specific generator for node type ${nodeType}, using SelectSQLGenerator`);
        return new SelectSQLGenerator(options);
    }
  }
}