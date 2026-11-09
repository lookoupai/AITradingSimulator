/* Shared controls and presentation for private/public PC28 roll simulations. */
class RollSimulationControls {
    constructor(prefix, onChange) {
        this.prefix = prefix;
        this.root = document.getElementById(`${prefix}RollControls`);
        this.selection = document.getElementById(`${prefix}PlaySelection`);
        this.selectionMode = document.getElementById(`${prefix}SelectionMode`);
        this.fixedValue = document.getElementById(`${prefix}FixedValue`);
        this.metric = document.getElementById(`${prefix}MetricView`);
        this.active = false;
        this.root?.addEventListener('change', () => {
            this.sync(this.active, this.predictor);
            if (this.validate()) onChange();
        });
        this.selection?.addEventListener('change', () => { this.sync(this.active, this.predictor); onChange(); });
        this.metric?.addEventListener('change', () => this.sync(this.active, this.predictor));
    }

    validate() {
        for (const field of this.root?.querySelectorAll('[data-roll-key]') || []) {
            if (!field.checkValidity()) { field.reportValidity(); return false; }
        }
        return true;
    }

    sync(active, predictor) {
        this.predictor = predictor;
        this.active = active;
        if (!this.root) return;
        this.root.hidden = !active;
        const structure = this.root.querySelector('[data-roll-key="structure"]');
        const capable = Boolean(predictor?.simulation_play_selection);
        const singleOption = structure.querySelector('[value="single_play"]');
        if (singleOption) singleOption.disabled = !capable;
        if (!capable && structure.value === 'single_play') structure.value = 'follow_signal';
        const single = structure.value === 'single_play';
        const enabled = capable && (!active || single);
        if (this.selection) this.selection.hidden = !enabled;
        if (this.metric && predictor) {
            const showMetric = (!active || single) && (predictor.simulation_play_metrics || predictor.simulation_metrics || []).length > 1;
            this.metric.hidden = !showMetric;
            this.metric.disabled = !showMetric;
        }
        const values = {big_small:['大','小'],odd_even:['单','双'],combo:['大单','大双','小单','小双'],kill_group:['大单','大双','小单','小双'],
            double_group:['大单+大双','大单+小单','大单+小双','大双+小单','大双+小双','小单+小双'],number:Array.from({length:28},(_,i)=>String(i))};
        if (this.fixedValue) {
            const canFollow = (predictor?.simulation_metrics || []).some(item => item.key === this.metric?.value);
            for (const option of this.selectionMode.options) option.disabled = option.value !== 'fixed' && !canFollow;
            if (!canFollow) this.selectionMode.value = 'fixed';
            const old = this.fixedValue.value;
            this.fixedValue.replaceChildren(...(values[this.metric?.value] || []).map(value => new Option((this.metric.value === 'kill_group' ? '杀' : '') + value, value)));
            if ((values[this.metric?.value] || []).includes(old)) this.fixedValue.value = old;
            this.fixedValue.parentElement.hidden = !enabled || this.selectionMode.value === 'follow';
        }
        this.root.querySelector('[data-roll-key="main_share"]').parentElement.hidden = single;
        this.root.querySelector('[data-roll-key="base_amount"]').min = single ? ({double_group:2,kill_group:3}[this.metric?.value] || 1) : 2;
        const supportsCombo = (predictor?.simulation_metrics || []).some(item => item.key === 'combo');
        structure.querySelector('[value="follow_signal"]').disabled = !supportsCombo;
        if (!supportsCombo && structure.value === 'follow_signal') structure.value = 'A';
    }

    query() {
        const values = new URLSearchParams();
        if (this.selection && !this.selection.hidden) {
            values.set('play_selection_mode', this.selectionMode.value);
            values.set('play_fixed_value', this.fixedValue.value);
        }
        if (!this.active) return values.size ? `&${values}` : '';
        for (const field of this.root.querySelectorAll('[data-roll-key]')) {
            const key = field.dataset.rollKey;
            values.set(key === 'roll_mode' ? 'roll_mode' : `roll_${key}`, key === 'main_share' ? String(Number(field.value) / 100) : field.value);
        }
        if (this.root.querySelector('[data-roll-key="structure"]').value === 'single_play') {
            values.set('roll_play_type', this.metric.value);
            values.set('roll_selection_mode', this.selectionMode.value);
            values.set('roll_fixed_value', this.fixedValue.value);
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
        return `整期起始 ${view.formatUsd(config.base_amount || 0)} · ` + (config.structure === 'single_play' ? '按所选玩法下注 · ' : `主注 ${Number((config.main_share || 0) * 100).toFixed(2)}% · `) +
            `每天最多 ${config.max_cycles_per_day || 1} 轮 · 跳过 ${summary.skipped_count || 0} 期（含收工后期号） · ${simulation.replay_note || ''}`;
    }

    static detail(item, view) {
        if (item.metric !== 'roll') return '';
        const stop = item.stop_reason === 'target' ? ' · 本轮达标' : item.stop_reason === 'bust' ? ' · 资金不足' : '';
        const text = `返还 ${view.formatUsd(item.payout_amount)} · 余额 ${view.formatUsd(item.balance_before)} → ${view.formatUsd(item.balance_after)}${stop}`;
        return `<span class="result-meta-text roll-record-detail">${view.escapeHtml(text)}</span>`;
    }
}
