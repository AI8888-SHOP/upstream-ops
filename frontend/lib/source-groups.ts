import type { RateSnapshot } from "@/lib/api-types"

type SourceGroupID = number | string | null | undefined

function normalizeSourceGroupName(value?: string) {
  return (value ?? "").trim().toLowerCase()
}

export function findSourceGroupRatio(
  groups: RateSnapshot[],
  sourceGroupID: SourceGroupID,
  sourceGroupName?: string,
): number | undefined {
  const numericID = sourceGroupID == null ? Number.NaN : Number(sourceGroupID)
  if (Number.isFinite(numericID) && numericID > 0) {
    const byID = groups.find(
      (group) =>
        group.remote_group_id != null && Number(group.remote_group_id) === numericID,
    )
    if (byID) return byID.ratio
  }

  const normalizedName = normalizeSourceGroupName(sourceGroupName)
  if (!normalizedName) return undefined
  return groups.find(
    (group) => normalizeSourceGroupName(group.model_name) === normalizedName,
  )?.ratio
}
