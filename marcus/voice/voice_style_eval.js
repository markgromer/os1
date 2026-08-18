const TRAINING_WHEELS = [
  /^(?:got it|sure(?: thing)?|absolutely|of course|certainly)[,!.\s]/i,
  /\b(?:i(?:'ll| will)|let me)\s+(?:first\s+)?(?:verify|check|review|make sure|ensure|carefully)/i,
  /\bget back to you\b/i,
  /\bfirst[, ]+i(?:'ll| will)\b/i,
  /\bmake sure (?:this|that|it|nothing) (?:doesn'?t|does not|won'?t|will not) (?:duplicate|conflict|break)/i,
  /\bto ensure (?:that )?(?:everything|nothing|this|it)\b/i,
];

export function evaluateMarcusVoiceStyle(value) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  const violations = TRAINING_WHEELS.filter((pattern) => pattern.test(text)).map((pattern) => pattern.source);
  return {
    passed: Boolean(text) && violations.length === 0,
    violations,
    text,
  };
}
