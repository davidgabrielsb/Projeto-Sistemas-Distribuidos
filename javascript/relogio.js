let contador = 0;
let diferenca = 0;
module.exports = {
    receber(valor) { if (Number.isSafeInteger(valor) && valor >= 0) contador = Math.max(contador, valor); },
    hora() { return Date.now() + diferenca; },
    ajustar(delta) { diferenca += delta; },
    metadados() { return { timestamp: new Date(Date.now() + diferenca).toISOString(), relogio_logico: ++contador }; }
};
