function clean(value, max = 400) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export function buildVoiceContinuityBrief({ memories = [], conversation = {}, personalityMode = 'operator' } = {}) {
  const publicMode = ['public_assistant', 'meeting_shadow'].includes(String(personalityMode));
  const active = (Array.isArray(memories) ? memories : []).filter((item) => item?.status === 'active');
  const priorities = active.filter((item) => ['mission', 'standing_instruction', 'preference'].includes(item.kind)
    && (!publicMode || !/relationship|voice|humor|joke|riff|teas|habit|working style/i.test(`${item.title} ${item.content}`))).slice(0, 5);
  const relationship = publicMode ? [] : active.filter((item) => /relationship|voice|humor|joke|riff|teas|habit|working style/i.test(`${item.title} ${item.content}`)).slice(0, 4);
  const recent = publicMode ? [] : (Array.isArray(conversation?.messages) ? conversation.messages : [])
    .filter((item) => ['user', 'assistant'].includes(item?.role))
    .slice(-4)
    .map((item) => `${item.role === 'user' ? 'Mark' : 'Marcus'}: ${clean(item.content, 240)}`);
  const lines = [
    clean(conversation?.activeProject?.name) ? `Active project: ${clean(conversation.activeProject.name, 120)}.` : '',
    ...priorities.map((item) => `Standing context: ${clean(item.title, 100)} — ${clean(item.content, 320)}`),
    ...relationship.map((item) => `Relationship context: ${clean(item.content, 320)}`),
    ...recent.map((item) => `Recent exchange: ${item}`),
  ].filter(Boolean);
  return lines.join('\n').slice(0, 3_500);
}
