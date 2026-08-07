import type { RateSnapshot } from "@/lib/api-types"

type SourceGroupID = number | string | null | undefined

function normalizeSourceGroupName(value?: string) {
  return (value ?? "").trim().toLowerCase()
}

/** Parse legacy route snapshots such as `id:63` / `source id: 63`. */
function parseSourceGroupIDRef(value?: string): number | undefined {
  const compact = (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
  const match = /^(?:id|sourceid|\u6e90id):([0-9]+)$/.exec(compact)
  if (!match) return undefined
  const id = Number(match[1])
  return Number.isSafeInteger(id) && id > 0 ? id : undefined
}

function finiteRatio(value: unknown): number | undefined {
  const ratio = Number(value)
  return Number.isFinite(ratio) ? ratio : undefined
}

export function findSourceGroupRatio(
  groups: RateSnapshot[],
  sourceGroupID: SourceGroupID,
  sourceGroupName?: string,
): number | undefined {
  const explicitID = sourceGroupID == null ? Number.NaN : Number(sourceGroupID)
  const numericID = Number.isFinite(explicitID) && explicitID > 0
    ? explicitID
    : parseSourceGroupIDRef(sourceGroupName) ?? Number.NaN
  if (Number.isFinite(numericID) && numericID > 0) {
    const byID = groups.find(
      (group) =>
        group.remote_group_id != null && Number(group.remote_group_id) === numericID,
    )
    const ratio = finiteRatio(byID?.ratio)
    if (ratio != null) return ratio
  }

  const normalizedName = normalizeSourceGroupName(sourceGroupName)
  if (!normalizedName) return undefined
  return finiteRatio(groups.find(
    (group) => normalizeSourceGroupName(group.model_name) === normalizedName,
  )?.ratio)
}
