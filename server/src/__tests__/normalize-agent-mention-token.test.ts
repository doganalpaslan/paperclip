import { describe, expect, it } from "vitest";
import { normalizeAgentMentionToken } from "../services/issues.ts";

describe("normalizeAgentMentionToken", () => {
  it("strips hex numeric entities such as space (&#x20;)", () => {
    expect(normalizeAgentMentionToken("Baba&#x20;")).toBe("Baba");
  });

  it("strips decimal numeric entities", () => {
    expect(normalizeAgentMentionToken("Baba&#32;")).toBe("Baba");
  });

  it("strips common named entities", () => {
    expect(normalizeAgentMentionToken("Baba&nbsp;")).toBe("Baba");
  });

  it("returns plain names unchanged", () => {
    expect(normalizeAgentMentionToken("Baba")).toBe("Baba");
  });

  it("trims after stripping entities", () => {
    expect(normalizeAgentMentionToken("Baba&#x20;&#x20;")).toBe("Baba");
  });
});
