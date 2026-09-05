import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createOperationsEngine } from '../../marcus/operations/operation_engine.js';
import { WorkGraph } from '../../marcus/work/work_graph.js';
import { WorkContextService } from '../../marcus/work/context_service.js';
import { MissionMemoryStore } from '../../marcus/memory/mission_memory_store.js';
import { EngineeringDirector } from '../../marcus/work/engineering_director.js';
import { HumanIdentityService } from '../../marcus/work/human_identity.js';
import { DurableExecution } from '../../marcus/work/durable_execution.js';
import { ProactiveOperator } from '../../marcus/work/proactive_operator.js';
import { AttentionStore } from '../../marcus/nervous_system/attention_store.js';
import { SignalBus } from '../../marcus/nervous_system/signal_bus.js';

export async function workFixture(options = {}) {
  const dataDir = options.dataDir || await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-domains-'));
  const engine = createOperationsEngine({ dataDir });
  const graph = new WorkGraph({ dataDir, engine }); const memory = new MissionMemoryStore({ dataDir });
  const context = new WorkContextService({ dataDir, graph, memory }); graph.decisions = context;
  const director = new EngineeringDirector({ dataDir, graph, context });
  const identities = new HumanIdentityService({ dataDir, graph }); const attention = new AttentionStore({ dataDir });
  const bus = new SignalBus();
  const execution = new DurableExecution({ dataDir, graph, director, bus, now: options.now, leaseMs: options.leaseMs });
  const operator = new ProactiveOperator({ dataDir, graph, execution, director, attention, now: options.now });
  engine.setWorkGuard(async (key, operation) => { await graph.assertOperationReady(key, operation); await director.assertOperationGrant(key, operation); });
  const project = await engine.registry.create('personal', { canonicalName: 'MARCUS constitution acceptance', projectId: 'marcus' });
  const other = await engine.registry.create('personal', { canonicalName: 'Separate private project', projectId: 'private' });
  const create = (objective, extra = {}) => graph.create('personal', { projectId: project.id, objective, acceptanceCriteria: [objective], ...extra });
  return { dataDir, engine, graph, memory, context, director, identities, attention, bus, execution, operator, project, other, create };
}
