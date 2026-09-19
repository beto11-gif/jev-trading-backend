import type { AnalysisResult } from '../../schemas/contracts.js';
import type { AnalysisContext } from '../market/processor.js';
export interface JevAnalyzer {
  readonly enabled: boolean;
  analyze(context: AnalysisContext, signal: AbortSignal): Promise<AnalysisResult>;
}
export class DisabledJevAnalyzer implements JevAnalyzer {
  readonly enabled = false;
  async analyze(): Promise<AnalysisResult> { throw new Error('JEV DISABLED / NOT CONFIGURED'); }
}
