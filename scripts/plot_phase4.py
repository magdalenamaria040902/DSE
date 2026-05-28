import pandas as pd
import matplotlib.pyplot as plt


try:
    df = pd.read_csv('events_results.csv')
except UnicodeDecodeError:
    df = pd.read_csv('events_results.csv', encoding='utf-16')

# Keep only numeric rows if terminal noise appears.
df = df[pd.to_numeric(df['end_to_end_ms'], errors='coerce').notna()].copy()
df['end_to_end_ms'] = df['end_to_end_ms'].astype(float)
df['work_ms'] = pd.to_numeric(df['work_ms'], errors='coerce').fillna(0)

# Histogram
plt.figure(figsize=(8, 4))
plt.hist(df['end_to_end_ms'], bins=30)
plt.title('Producer/Consumer End-to-End Latency Distribution')
plt.xlabel('Latency (ms)')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig('p4_latency_chart.png', dpi=150)
plt.close()

# Boxplot
plt.figure(figsize=(6, 4))
plt.boxplot(df['end_to_end_ms'], vert=True)
plt.title('Producer/Consumer End-to-End Latency Boxplot')
plt.ylabel('Latency (ms)')
plt.tight_layout()
plt.savefig('p4_latency_boxplot.png', dpi=150)
plt.close()

# Percentiles bar
p50 = df['end_to_end_ms'].quantile(0.50)
p95 = df['end_to_end_ms'].quantile(0.95)
p99 = df['end_to_end_ms'].quantile(0.99)

plt.figure(figsize=(6, 4))
plt.bar(['p50', 'p95', 'p99'], [p50, p95, p99])
plt.title('Producer/Consumer Latency Percentiles')
plt.ylabel('Latency (ms)')
plt.tight_layout()
plt.savefig('p4_latency_percentiles.png', dpi=150)
plt.close()

# Work-vs-end-to-end scatter
plt.figure(figsize=(7, 4))
plt.scatter(df['work_ms'], df['end_to_end_ms'], s=8, alpha=0.6)
plt.title('Work Time vs End-to-End Latency')
plt.xlabel('Simulated Work (ms)')
plt.ylabel('End-to-End Latency (ms)')
plt.tight_layout()
plt.savefig('p4_work_vs_latency.png', dpi=150)
plt.close()

print('Generated charts: p4_latency_chart.png, p4_latency_boxplot.png, p4_latency_percentiles.png, p4_work_vs_latency.png')
print(f'Rows plotted: {len(df)}')
print(f'p50={p50:.2f} ms, p95={p95:.2f} ms, p99={p99:.2f} ms')
