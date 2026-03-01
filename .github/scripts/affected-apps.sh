# Get affected projects
affected_apps=$(bunx nx show projects --affected --base=origin/main~1 --head=origin/main --type=app --json)

# Remove @ts-backend/ prefix from package names
affected_apps=$(echo "$affected_apps" | sed 's/"@ts-backend\//"/'g)

echo "Affected apps: $affected_apps"

# Create matrix for parallel jobs
if [ -n "$affected_apps" ] && [ "$affected_apps" != "[]" ] && [ "$affected_apps" != "" ]; then
  has_changes="true"
  # The output is already a JSON array, just wrap it in the matrix structure
  matrix_json="{\"app\":$affected_apps}"
else
  has_changes="false"
  matrix_json="{\"app\":[]}"
fi

echo "Matrix JSON: $matrix_json"
echo "Has changes: $has_changes"

echo "apps=$affected_apps" >> $GITHUB_OUTPUT
echo "matrix=$matrix_json" >> $GITHUB_OUTPUT
echo "has-changes=$has_changes" >> $GITHUB_OUTPUT