/* Shared controls and presentation for private/public PC28 roll simulations. */
class RollSimulationControls {
    constructor(prefix, onChange) {
        this.root = document.getElementById(`${prefix}RollControls`);
        this.active = false;
        this.root?.addEventListener('change', () => {
            if (this.validate()) onChange();
        });
    }

    validate() {
        for (const field of this.root?.querySelectorAll('[data-roll-key]') || []) {
            if (!field.checkValidity()) { field.reportValidity(); return false; }
        }
        return true;
    }

    sync(active, predictor) {
        this.active = active;
        if (!this.root) return;
        this.root.hidden = !active;
        const structure = this.root.querySelector('[data-roll-key="structure"]');
        const supportsCombo = (predictor?.simulation_metrics || []).some(item => item.key === 'combo');
        structure.querySelector('[value="follow_signal"]').disabled = !supportsCombo;
        if (!supportsCombo && structure.value === 'follow_signal') structure.value = 'A';
    }

    query() {
        if (!this.active) return '';
        const values = new URLSearchParams();
        for (const field of this.root.querySelectorAll('[data-roll-key]')) {
            const key = field.dataset.rollKey;
            values.set(key === 'roll_mode' ? 'roll_mode' : `roll_${key}`, key === 'main_share' ? String(Number(field.value) / 100) : field.value);
        }
        return `&${values}`;
    }

    static summaryCards(simulation, view) {
        const s = simulation.summary || {};
        return [
            ['净收益', view.formatSignedUsd(s.net_profit || 0)],
            ['ROI', view.formatPercent(s.roi_percentage || 0)],
            ['总下注', view.formatUsd(s.total_stake || 0)],
            ['下注期数', s.bet_count || 0],
            ['达标 / 资金不足 / 未完成轮数', `${s.target_cycle_count || 0} / ${s.bust_cycle_count || 0} / ${s.incomplete_cycle_count || 0}`],
            ['盈利 / 持平 / 亏损期数', `${s.hit_count || 0} / ${s.refund_count || 0} / ${s.miss_count || 0}`],
            ['累计投入本金', view.formatUsd(s.total_funding || 0)],
            ['重置时提取', view.formatUsd(s.total_withdrawn || 0)],
            ['期末账户余额', view.formatUsd(s.ending_balance || 0)],
            ['最大累计收益回撤', view.formatUsd(s.max_drawdown || 0)],
        ];
    }

    static hint(simulation, view) {
        const config = simulation.roll_config || {};
        const summary = simulation.summary || {};
        return `起始 ${view.formatUsd(config.base_amount || 0)} · 主注 ${Number((config.main_share || 0) * 100).toFixed(2)}% · ` +
            `每天最多 ${config.max_cycles_per_day || 1} 轮 · 跳过 ${summary.skipped_count || 0} 期（含收工后期号） · ${simulation.replay_note || ''}`;
    }

    static detail(item, view) {
        if (item.metric !== 'roll') return '';
        const stop = item.stop_reason === 'target' ? ' · 本轮达标' : item.stop_reason === 'bust' ? ' · 资金不足' : '';
        const text = `返还 ${view.formatUsd(item.payout_amount)} · 余额 ${view.formatUsd(item.balance_before)} → ${view.formatUsd(item.balance_after)}${stop}`;
        return `<span class="result-meta-text roll-record-detail">${view.escapeHtml(text)}</span>`;
    }
}
