import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export type MarcusModuleId =
  | 'operator_profile'
  | 'operational_doctrine'
  | 'personality_layer'
  | 'attention_radar'
  | 'strategic_forecasting'
  | 'execution_authority'
  | 'knowledge_archive'
  | 'daily_operations_rhythm';

export interface MarcusPromptModule {
  id: MarcusModuleId | string;
  title: string;
  fileName: string;
  overrideKeys?: string[];
}

export interface MarcusPromptInsertion {
  module: MarcusPromptModule;
  beforeId?: string;
  afterId?: string;
  index?: number;
}

export interface BuildMarcusSystemPromptOptions {
  uiOverrides?: Record<string, string | undefined | null>;
  insertions?: MarcusPromptInsertion[];
  modules?: MarcusPromptModule[];
  customSystemPrompt?: string;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const CORE_DIR = __dirname;

export const DEFAULT_MARCUS_MODULES: MarcusPromptModule[] = [
  { id: 'operator_profile', title: 'Operator Profile', fileName: 'operator_profile.md', overrideKeys: ['operator_profile', 'operatorBio'] },
  { id: 'operational_doctrine', title: 'Operational Doctrine', fileName: 'operational_doctrine.md', overrideKeys: ['operational_doctrine', 'assistantOperatingDoctrine', 'operatorHelpPrompt'] },
  { id: 'personality_layer', title: 'Personality Layer', fileName: 'personality_layer.md', overrideKeys: ['personality_layer', 'personalityLayer'] },
  { id: 'attention_radar', title: 'Attention Radar', fileName: 'attention_radar.md', overrideKeys: ['attention_radar', 'attentionRadar'] },
  { id: 'strategic_forecasting', title: 'Strategic Forecasting', fileName: 'strategic_forecasting.md', overrideKeys: ['strategic_forecasting'] },
  { id: 'execution_authority', title: 'Execution Authority', fileName: 'execution_authority.md', overrideKeys: ['execution_authority'] },
  { id: 'knowledge_archive', title: 'Knowledge Archive', fileName: 'knowledge_archive.md', overrideKeys: ['knowledge_archive'] },
  { id: 'daily_operations_rhythm', title: 'Daily Operations Rhythm', fileName: 'daily_operations_rhythm.md', overrideKeys: ['daily_operations_rhythm', 'dailyReportingStructure'] },
];

function normalizeText(input?: string | null): string {
  return typeof input === 'string' ? input.trim() : '';
}

function cloneModules(modules: MarcusPromptModule[]): MarcusPromptModule[] {
  return modules.map((module) => ({ ...module, overrideKeys: [...(module.overrideKeys || [])] }));
}

function insertDynamicModules(baseModules: MarcusPromptModule[], insertions: MarcusPromptInsertion[] = []): MarcusPromptModule[] {
  const next = [...baseModules];
  for (const insertion of insertions) {
    if (!insertion || !insertion.module) continue;
    const module = { ...insertion.module, overrideKeys: [...(insertion.module.overrideKeys || [])] };

    if (Number.isInteger(insertion.index)) {
      const index = Math.max(0, Math.min(next.length, Number(insertion.index)));
      next.splice(index, 0, module);
      continue;
    }

    if (insertion.beforeId) {
      const index = next.findIndex((item) => item.id === insertion.beforeId);
      if (index >= 0) {
        next.splice(index, 0, module);
        continue;
      }
    }

    if (insertion.afterId) {
      const index = next.findIndex((item) => item.id === insertion.afterId);
      if (index >= 0) {
        next.splice(index + 1, 0, module);
        continue;
      }
    }

    next.push(module);
  }
  return next;
}

async function loadModuleMarkdown(module: MarcusPromptModule): Promise<string> {
  const fullPath = path.join(CORE_DIR, module.fileName);
  const content = await fs.readFile(fullPath, 'utf8');
  return normalizeText(content);
}

function resolveOverrideContent(module: MarcusPromptModule, uiOverrides: Record<string, string | undefined | null> = {}): string {
  const overrideKeys = [module.id, ...(module.overrideKeys || [])];
  for (const key of overrideKeys) {
    const value = normalizeText(uiOverrides[key]);
    if (value) return value;
  }
  return '';
}

export async function buildMarcusSystemPrompt(options: BuildMarcusSystemPromptOptions = {}): Promise<string> {
  const uiOverrides = options.uiOverrides || {};
  const baseModules = cloneModules(options.modules || DEFAULT_MARCUS_MODULES);
  const modules = insertDynamicModules(baseModules, options.insertions || []);

  const sections: string[] = ['### M.A.R.C.U.S. CORE SYSTEM'];
  const customSystemPrompt = normalizeText(options.customSystemPrompt);
  if (customSystemPrompt) {
    sections.push(customSystemPrompt);
  }

  for (const module of modules) {
    const overrideContent = resolveOverrideContent(module, uiOverrides);
    const content = overrideContent || await loadModuleMarkdown(module);
    sections.push(`## ${module.title}\n${content}`);
  }

  return sections.join('\n\n').trim();
}
