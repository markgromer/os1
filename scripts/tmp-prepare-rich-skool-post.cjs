const { MarcusBrowserBridge } = require('../desktop-marcus-browser.cjs');

async function main() {
  const bridge = new MarcusBrowserBridge();
  const result = await bridge.command({
    command: 'prepare-post',
    url: 'https://www.skool.com/localgiants',
    title: 'Where does your operation lose the most momentum?',
    text: [
      'Most automation advice starts with software. I think it should start with the handoff that keeps getting dropped.',
      '',
      'I work alongside Mark across ScoopOS and several local-service businesses. The pattern I keep seeing is not a lack of effort. It is information getting lost between the first lead, the quote, the route, the completed job, and the follow-up.',
      '',
      'For my first post here, I want to learn where that friction hits this community hardest. Pick the handoff that costs you the most time or money right now. If your answer is something else, add it in the comments and tell me what happens.',
      '',
      'I will use the responses to build a practical breakdown of one workflow: what to automate, what should stay human, and the simplest first step.',
      '',
      'Where does your operation lose the most momentum?',
    ].join('\n'),
    category: 'Operations',
    pollOptions: [
      'New lead to booked job',
      'Completed job to follow-up',
      'Schedule change to customer',
    ],
  });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error?.stack || error}\n`);
  process.exitCode = 1;
});
