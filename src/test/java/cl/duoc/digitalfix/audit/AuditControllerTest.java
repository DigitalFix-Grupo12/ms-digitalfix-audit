package cl.duoc.digitalfix.audit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Pruebas de integracion con H2: ingesta y consulta del timeline. */
@SpringBootTest
@AutoConfigureMockMvc
class AuditControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void registraYConsultaEventoPorReferencia() throws Exception {
        mockMvc.perform(post("/api/audit")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {"usuario":"supervisor@test","accion":"ASIGNO orden #77 a t1",
                     "servicio":"ms-digitalfix-workorders","referencia":"orden#77"}
                    """))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").isNumber())
            .andExpect(jsonPath("$.fecha").exists());

        mockMvc.perform(get("/api/audit").param("referencia", "orden#77"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].usuario").value("supervisor@test"))
            .andExpect(jsonPath("$[0].accion").value("ASIGNO orden #77 a t1"));
    }

    @Test
    void eventoSinUsuarioDevuelve400() throws Exception {
        mockMvc.perform(post("/api/audit")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"accion\":\"x\",\"servicio\":\"y\"}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void limiteSeAcota() throws Exception {
        mockMvc.perform(get("/api/audit").param("limit", "100000"))
            .andExpect(status().isOk());
    }
}
