import { ApiError } from "@/api/client";

import { createModelLifecycleMock } from "./modelLifecycleMock";

describe("model lifecycle mock", () => {
  it("requires current revisions and explicit operator confirmation", () => {
    const mock = createModelLifecycleMock();
    const profile = mock.list().items.find(
      (item) => item.state === "certified",
    )!;

    expect(() =>
      mock.transition(
        profile.profile_id,
        {
          target_state: "promoted",
          expected_revision: profile.revision + 1,
          reason_code: "operator.promote",
          operator_confirmed: true,
        },
        "operator",
      ),
    ).toThrowError(ApiError);
    expect(() =>
      mock.transition(
        profile.profile_id,
        {
          target_state: "promoted",
          expected_revision: profile.revision,
          reason_code: "operator.promote",
          operator_confirmed: false,
        },
        "operator",
      ),
    ).toThrowError(ApiError);
  });

  it("promotes with a rollback target and rolls back atomically", () => {
    const mock = createModelLifecycleMock();
    const source = mock.list().items.find(
      (item) => item.state === "certified",
    )!;
    const target = mock.list().items.find(
      (item) => item.state === "promoted",
    )!;
    const promoted = mock.transition(
      source.profile_id,
      {
        target_state: "promoted",
        expected_revision: source.revision,
        rollback_profile_id: target.profile_id,
        reason_code: "operator.promote",
        operator_confirmed: true,
      },
      "operator",
    );

    expect(promoted.state).toBe("promoted");
    expect(promoted.rollback_profile_id).toBe(target.profile_id);
    const restored = mock.rollback(
      promoted.profile_id,
      {
        expected_revision: promoted.revision,
        reason_code: "operator.rollback",
        operator_confirmed: true,
      },
      "operator",
    );
    expect(restored.profile_id).toBe(target.profile_id);
    expect(restored.state).toBe("promoted");
    expect(
      mock
        .list()
        .items.find((item) => item.profile_id === source.profile_id)
        ?.state,
    ).toBe("quarantined");
  });

  it("records a bounded passing smoke without clearing quarantine", () => {
    const mock = createModelLifecycleMock();
    const profile = mock.list().items.find(
      (item) => item.state === "quarantined",
    )!;
    const result = mock.smoke(profile.profile_id);

    expect(result.overall_ok).toBe(true);
    expect(result.result_snapshot).toMatchObject({
      calls_attempted: 3,
      cluster_mutation_performed: false,
    });
    expect(
      mock
        .list()
        .items.find((item) => item.profile_id === profile.profile_id)
        ?.state,
    ).toBe("quarantined");
  });
});
